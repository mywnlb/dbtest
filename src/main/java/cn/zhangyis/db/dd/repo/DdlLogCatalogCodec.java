package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * DDL phase marker 与 InternalCatalogStore 无语义 record 的版本化 codec。key/payload 重复 identity，
 * 解码逐字段交叉校验，避免单侧损坏被恢复状态机接受。
 */
final class DdlLogCatalogCodec {

    /** kind/operation/ddl/version/phase/chunk 的固定 big-endian key 长度。 */
    private static final int KEY_V1_BYTES = 1 + Long.BYTES * 3 + Integer.BYTES * 2;
    private static final int KEY_V2_BYTES = KEY_V1_BYTES + Long.BYTES;
    /** payload v1 魔数 "DDL1"。 */
    private static final int MAGIC = 0x44444C31; // DDL1
    /** 当前写格式；v2 在 key/payload 同时加入 secondaryObjectId。 */
    private static final int FORMAT_VERSION = 2;
    /** CREATE/DROP TABLE 历史 marker 的兼容读格式。 */
    private static final int LEGACY_FORMAT_VERSION = 1;
    /** 为 catalog record payload 头与未来扩展保留空间后的 UTF-8 路径上限。 */
    private static final int MAX_PATH_BYTES = 900;

    /**
     * 编码单条 DDL marker；InternalCatalogStore 的一个 append 批次即一个 phase 原子提交。
     *
     * @param record 已验证且路径规范化的 marker。
     * @return key/payload 均独立复制的 catalog record。
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException UTF-8 路径超过 v1 上限时抛出。
     */
    CatalogRecord encode(DdlLogRecord record) {
        byte[] path = record.path().toString().getBytes(StandardCharsets.UTF_8);
        if (path.length > MAX_PATH_BYTES) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "DDL log path exceeds v1 limit: " + path.length);
        }
        DdlUndoMarker marker = record.marker();
        ByteBuffer key = ByteBuffer.allocate(KEY_V2_BYTES).order(ByteOrder.BIG_ENDIAN)
                .put((byte) CatalogEntityKind.DDL_LOG.stableCode())
                .putLong(record.operation().stableCode())
                .putLong(marker.ddlOperationId())
                .putLong(marker.dictionaryVersion())
                .putLong(record.secondaryObjectId())
                .putInt(record.phase().stableCode())
                .putInt(0);
        ByteBuffer payload = ByteBuffer.allocate(Integer.BYTES + 1 + Long.BYTES * 4 + 1 + 1
                        + Integer.BYTES + Short.BYTES + path.length)
                .order(ByteOrder.BIG_ENDIAN);
        payload.putInt(MAGIC).put((byte) FORMAT_VERSION)
                .putLong(marker.ddlOperationId()).putLong(marker.dictionaryVersion())
                .putLong(marker.affectedObjectId())
                .putLong(record.secondaryObjectId())
                .put((byte) record.operation().stableCode()).put((byte) record.phase().stableCode())
                .putInt(record.spaceId().value()).putShort((short) path.length).put(path);
        return new CatalogRecord(key.array(), payload.array());
    }

    /**
     * 尝试解码一个 committed batch。普通 DD/future batch 返回 empty；DDL_LOG 形状损坏则 fail-closed。
     *
     * @param batch 已经 storage frame CRC/SHA/header 边界验证的原子批次。
     * @return DDL marker，或当前批次不属于 DDL log 时为空。
     * @throws DictionaryCatalogCorruptionException DDL key/payload/version/identity/path 不可解释时抛出。
     */
    Optional<DdlLogRecord> decode(CatalogBatch batch) {
        List<CatalogRecord> records = batch.records();
        byte[] firstKey = records.getFirst().key();
        if (Byte.toUnsignedInt(firstKey[0]) != CatalogEntityKind.DDL_LOG.stableCode()) {
            return Optional.empty();
        }
        if (firstKey.length != KEY_V1_BYTES && firstKey.length != KEY_V2_BYTES) {
            throw new DictionaryCatalogCorruptionException("DDL log key length is invalid: " + firstKey.length);
        }
        if (records.size() != 1) {
            throw new DictionaryCatalogCorruptionException("DDL log batch must contain exactly one record");
        }
        ByteBuffer key = ByteBuffer.wrap(firstKey).order(ByteOrder.BIG_ENDIAN);
        key.get();
        long keyOperation = key.getLong();
        long keyDdlId = key.getLong();
        long keyVersion = key.getLong();
        long keySecondaryObjectId = firstKey.length == KEY_V2_BYTES ? key.getLong() : 0L;
        int keyPhase = key.getInt();
        int chunk = key.getInt();
        if (chunk != 0) {
            throw new DictionaryCatalogCorruptionException("DDL log v1 does not support payload chunks");
        }

        byte[] bytes = records.getFirst().payload();
        try {
            ByteBuffer payload = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            if (payload.remaining() < Integer.BYTES + 1 + Long.BYTES * 3 + 1 + 1
                    + Integer.BYTES + Short.BYTES || payload.getInt() != MAGIC) {
                throw new DictionaryCatalogCorruptionException("invalid DDL log payload header/version");
            }
            int format = Byte.toUnsignedInt(payload.get());
            if (format != LEGACY_FORMAT_VERSION && format != FORMAT_VERSION
                    || format == LEGACY_FORMAT_VERSION && firstKey.length != KEY_V1_BYTES
                    || format == FORMAT_VERSION && firstKey.length != KEY_V2_BYTES) {
                throw new DictionaryCatalogCorruptionException("invalid DDL log payload header/version");
            }
            long ddlId = payload.getLong();
            long version = payload.getLong();
            long objectId = payload.getLong();
            long secondaryObjectId = format == FORMAT_VERSION ? payload.getLong() : 0L;
            int operationCode = Byte.toUnsignedInt(payload.get());
            int phaseCode = Byte.toUnsignedInt(payload.get());
            int spaceId = payload.getInt();
            int pathLength = Short.toUnsignedInt(payload.getShort());
            if (pathLength > MAX_PATH_BYTES || pathLength != payload.remaining()) {
                throw new DictionaryCatalogCorruptionException("DDL log path length/trailing bytes mismatch");
            }
            byte[] pathBytes = new byte[pathLength];
            payload.get(pathBytes);
            if (keyOperation != operationCode || keyDdlId != ddlId || keyVersion != version
                    || keySecondaryObjectId != secondaryObjectId || keyPhase != phaseCode) {
                throw new DictionaryCatalogCorruptionException("DDL log key/payload identity mismatch");
            }
            String path = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(pathBytes)).toString();
            return Optional.of(new DdlLogRecord(new DdlUndoMarker(ddlId, version, objectId), secondaryObjectId,
                    DdlLogOperation.fromStableCode(operationCode), DdlLogPhase.fromStableCode(phaseCode),
                    SpaceId.of(spaceId), java.nio.file.Path.of(path)));
        } catch (CharacterCodingException | java.nio.BufferUnderflowException | java.nio.InvalidMarkException error) {
            throw new DictionaryCatalogCorruptionException("truncated/invalid DDL log payload", error);
        } catch (java.nio.file.InvalidPathException error) {
            throw new DictionaryCatalogCorruptionException("invalid DDL log path", error);
        } catch (cn.zhangyis.db.common.exception.DatabaseValidationException error) {
            throw new DictionaryCatalogCorruptionException("invalid DDL log domain identity", error);
        }
    }
}
