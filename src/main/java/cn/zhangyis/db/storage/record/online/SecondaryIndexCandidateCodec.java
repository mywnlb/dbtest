package cn.zhangyis.db.storage.record.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexCandidate;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexCandidateCodec;
import cn.zhangyis.db.storage.btree.SearchKeyComparator;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordDecoder;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.schema.SecondaryIndexLayout;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * 基于 {@link SecondaryIndexLayout} 的 candidate v1 codec。payload 内只保存非聚簇 physical entry；row-log
 * 外层 frame 已提供 length、identity、sequence 和 CRC32C，因此本格式不重复校验和。
 */
public final class SecondaryIndexCandidateCodec implements OnlineIndexCandidateCodec {

    /** ASCII "OIDC"，用于拒绝把其它 opaque frame 当作 candidate。 */
    private static final int MAGIC = 0x4f494443;
    /** 当前稳定格式版本。 */
    private static final short VERSION = 1;
    /** 固定前缀：magic/version/flags/reserved/before length/after length。 */
    private static final int PREFIX_BYTES = Integer.BYTES + Short.BYTES + 2 + Integer.BYTES * 2;
    /** before entry 在 flags 中的存在位。 */
    private static final byte BEFORE_PRESENT = 1;
    /** after entry 在 flags 中的存在位。 */
    private static final byte AFTER_PRESENT = 2;

    /** build manifest 冻结的完整表到 secondary entry 投影。 */
    private final SecondaryIndexLayout layout;
    /** 使用项目 record 格式编码 entry，避免创建第二套字段编码。 */
    private final RecordEncoder encoder;
    /** 与 encoder 对称的 reconciliation 解码器。 */
    private final RecordDecoder decoder;
    /** 判断 UPDATE physical key 是否真正变化，遵守 prefix/collation/ASC-DESC 规则。 */
    private final SearchKeyComparator comparator;

    /**
     * 构造 immutable codec；registry 必须与 B+Tree 和 manifest row format 使用同一字符/类型语义。
     *
     * @param layout 当前 build 冻结的 secondary physical layout
     * @param registry StorageEngine 组合根共享的稳定类型 codec registry
     */
    public SecondaryIndexCandidateCodec(SecondaryIndexLayout layout, TypeCodecRegistry registry) {
        if (layout == null || registry == null) {
            throw new DatabaseValidationException("secondary candidate layout/registry must not be null");
        }
        this.layout = layout;
        this.encoder = new RecordEncoder(registry);
        this.decoder = new RecordDecoder(registry);
        this.comparator = new SearchKeyComparator(registry);
    }

    /** 把 INSERT 完整用户行投影为唯一 after physical entry。 */
    @Override
    public Optional<byte[]> encodeInsert(LogicalRecord after) {
        if (after == null) {
            throw new DatabaseValidationException("online index INSERT candidate row must not be null");
        }
        return Optional.of(encode(null, layout.toEntry(after, false)));
    }

    /**
     * 比较 before/after 的完整 physical key，仅在 prefix/collation 语义下发生真实键变化时编码两侧。
     */
    @Override
    public Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after) {
        if (before == null || after == null) {
            throw new DatabaseValidationException("online index UPDATE candidate rows must not be null");
        }
        LogicalRecord beforeEntry = layout.toEntry(before, false);
        LogicalRecord afterEntry = layout.toEntry(after, false);
        if (comparator.compare(layout.physicalKey(beforeEntry), layout.physicalKey(afterEntry),
                layout.physicalKeyDef(), layout.entrySchema()) == 0) {
            return Optional.empty();
        }
        return Optional.of(encode(beforeEntry, afterEntry));
    }

    /** 把 DELETE 权威旧行投影为唯一 before physical entry。 */
    @Override
    public Optional<byte[]> encodeDelete(LogicalRecord before) {
        if (before == null) {
            throw new DatabaseValidationException("online index DELETE candidate row must not be null");
        }
        return Optional.of(encode(layout.toEntry(before, false), null));
    }

    /**
     * 严格解码一个完整 payload；未知 flag、长度不一致和 entry 格式损坏均 fail-closed。
     *
     * @param payload row-log CANDIDATE frame 的完整 opaque payload
     * @return 至少携带一侧的 immutable candidate
     * @throws DatabaseValidationException magic/version/flags/length 或 record 格式非法时抛出
     */
    @Override
    public OnlineIndexCandidate decode(byte[] payload) {
        if (payload == null || payload.length < PREFIX_BYTES) {
            throw new DatabaseValidationException("online index candidate payload is truncated");
        }
        ByteBuffer input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int magic = input.getInt();
        short version = input.getShort();
        byte flags = input.get();
        byte reserved = input.get();
        int beforeLength = input.getInt();
        int afterLength = input.getInt();
        if (magic != MAGIC || version != VERSION || reserved != 0
                || (flags & ~(BEFORE_PRESENT | AFTER_PRESENT)) != 0
                || flags == 0 || beforeLength < 0 || afterLength < 0
                || ((flags & BEFORE_PRESENT) == 0) != (beforeLength == 0)
                || ((flags & AFTER_PRESENT) == 0) != (afterLength == 0)
                || beforeLength > input.remaining()
                || afterLength != input.remaining() - beforeLength) {
            throw new DatabaseValidationException("online index candidate header/length is invalid");
        }
        Optional<LogicalRecord> before = beforeLength == 0 ? Optional.empty()
                : Optional.of(decoder.decode(read(input, beforeLength), layout.entrySchema()));
        Optional<LogicalRecord> after = afterLength == 0 ? Optional.empty()
                : Optional.of(decoder.decode(read(input, afterLength), layout.entrySchema()));
        return new OnlineIndexCandidate(before, after);
    }

    /** 编码已经投影的可选 entry；方法不访问外部状态且返回独立字节数组。 */
    private byte[] encode(LogicalRecord before, LogicalRecord after) {
        byte[] beforeBytes = before == null ? new byte[0] : encoder.encode(before, layout.entrySchema());
        byte[] afterBytes = after == null ? new byte[0] : encoder.encode(after, layout.entrySchema());
        byte flags = 0;
        if (before != null) {
            flags |= BEFORE_PRESENT;
        }
        if (after != null) {
            flags |= AFTER_PRESENT;
        }
        return ByteBuffer.allocate(PREFIX_BYTES + beforeBytes.length + afterBytes.length)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).putShort(VERSION).put(flags).put((byte) 0)
                .putInt(beforeBytes.length).putInt(afterBytes.length)
                .put(beforeBytes).put(afterBytes).array();
    }

    /** 从当前位置复制精确长度，调用方已经验证 remaining。 */
    private static byte[] read(ByteBuffer input, int length) {
        byte[] bytes = new byte[length];
        input.get(bytes);
        return bytes;
    }
}
