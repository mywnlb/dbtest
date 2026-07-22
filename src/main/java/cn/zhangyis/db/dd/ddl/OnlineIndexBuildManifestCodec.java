package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Online ADD INDEX manifest v1 的显式稳定编码；不使用 Java serialization 或 enum ordinal。 */
public final class OnlineIndexBuildManifestCodec {

    private static final int MAGIC = 0x4f49444d; // OIDM
    private static final short VERSION = 1;
    private static final int FIXED_BYTES = Integer.BYTES + Short.BYTES + Short.BYTES
            + Long.BYTES * 5 + 1 + Integer.BYTES + Integer.BYTES;
    private static final int PART_BYTES = Long.BYTES + 1 + Integer.BYTES;

    /**
     * 把 immutable manifest 编码为可由 row-log SHA-256 绑定的独立字节数组。
     *
     * @param manifest initial X 下冻结的 build/table/version/index 完整恢复命令
     * @return 不依赖 Java serialization 或 enum ordinal 的 v1 独立字节数组
     * @throws DatabaseValidationException manifest 为空或编码总长溢出时抛出，调用方不得写 PREPARED marker
     */
    public byte[] encode(OnlineIndexBuildManifest manifest) {
        if (manifest == null) {
            throw new DatabaseValidationException("online index manifest must not be null");
        }
        byte[] name = manifest.index().name().displayName().getBytes(StandardCharsets.UTF_8);
        int size;
        try {
            size = Math.addExact(FIXED_BYTES + name.length,
                    Math.multiplyExact(manifest.index().keyParts().size(), PART_BYTES));
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("online index manifest length overflows", overflow);
        }
        ByteBuffer output = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putShort(VERSION).putShort((short) 0)
                .putLong(manifest.buildId().value())
                .putLong(manifest.tableId().value())
                .putLong(manifest.sourceVersion().value())
                .putLong(manifest.targetVersion().value())
                .putLong(manifest.index().id().value())
                .put((byte) (manifest.index().unique() ? 1 : 0))
                .putInt(name.length).put(name)
                .putInt(manifest.index().keyParts().size());
        for (IndexKeyPart part : manifest.index().keyParts()) {
            output.putLong(part.columnId())
                    .put((byte) (part.order() == IndexOrder.ASC ? 1 : 2))
                    .putInt(part.prefixBytes());
        }
        return output.array();
    }

    /**
     * 严格解码完整 manifest；未知版本/order、截断、尾随字节或非法 DD definition 均拒绝恢复。
     *
     * @param bytes row-log header 已完成 SHA-256 校验的完整 manifest bytes
     * @return 重建后的 immutable DD definition
     * @throws DatabaseValidationException magic、版本、长度、identity、order 或尾随字节非法时抛出并阻止恢复
     */
    public OnlineIndexBuildManifest decode(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_BYTES) {
            throw new DatabaseValidationException("online index manifest is truncated");
        }
        ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getShort() != VERSION || input.getShort() != 0) {
            throw new DatabaseValidationException("online index manifest magic/version is invalid");
        }
        long buildId = input.getLong();
        long tableId = input.getLong();
        long sourceVersion = input.getLong();
        long targetVersion = input.getLong();
        long indexId = input.getLong();
        byte unique = input.get();
        int nameLength = input.getInt();
        if (unique < 0 || unique > 1 || nameLength <= 0 || nameLength > input.remaining() - Integer.BYTES) {
            throw new DatabaseValidationException("online index manifest name/unique fields are invalid");
        }
        byte[] name = new byte[nameLength];
        input.get(name);
        int partCount = input.getInt();
        if (partCount <= 0 || partCount > input.remaining() / PART_BYTES
                || input.remaining() != partCount * PART_BYTES) {
            throw new DatabaseValidationException("online index manifest key part length is invalid");
        }
        List<IndexKeyPart> parts = new ArrayList<>(partCount);
        for (int i = 0; i < partCount; i++) {
            long columnId = input.getLong();
            byte orderCode = input.get();
            IndexOrder order = switch (orderCode) {
                case 1 -> IndexOrder.ASC;
                case 2 -> IndexOrder.DESC;
                default -> throw new DatabaseValidationException(
                        "unknown online index manifest order code: " + orderCode);
            };
            parts.add(new IndexKeyPart(columnId, order, input.getInt()));
        }
        IndexDefinition index = new IndexDefinition(IndexId.of(indexId),
                ObjectName.of(decodeUtf8(name)), unique == 1, false, parts);
        return new OnlineIndexBuildManifest(OnlineIndexBuildId.of(buildId), TableId.of(tableId),
                DictionaryVersion.of(sourceVersion), DictionaryVersion.of(targetVersion), index);
    }

    /** 严格解码持久对象名；replacement 字符会改变恢复 identity，因此 malformed 输入必须 fail-closed。 */
    private static String decodeUtf8(byte[] encoded) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
        } catch (CharacterCodingException malformed) {
            throw new DatabaseValidationException(
                    "online index manifest name is not strict UTF-8", malformed);
        }
    }
}
