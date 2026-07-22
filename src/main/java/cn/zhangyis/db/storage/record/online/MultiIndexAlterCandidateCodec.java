package cn.zhangyis.db.storage.record.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCandidate;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCandidateCodec;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCandidateEntry;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterIndexTarget;
import cn.zhangyis.db.storage.record.format.LogicalRecord;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 通用INPLACE多ADD INDEX candidate v1 codec。每次DML只append一个外层frame，frame内部按manifest action
 * ordinal携带所有受影响目标，因而transaction force high-water和gate owner不会被拆成多个不一致时点。
 */
public final class MultiIndexAlterCandidateCodec implements OnlineAlterCandidateCodec {

    /** ASCII `OAMC`，拒绝把shadow identity或旧单索引payload误解为多目标candidate。 */
    private static final int MAGIC = 0x4f414d43;
    /** 当前稳定版本。 */
    private static final short VERSION = 1;
    /** magic/version/flags/entry count。 */
    private static final int PREFIX_BYTES = Integer.BYTES + Short.BYTES * 2 + Integer.BYTES;
    /** ordinal/index id/payload length。 */
    private static final int ENTRY_PREFIX_BYTES = Integer.BYTES + Long.BYTES + Integer.BYTES;
    /** 与manifest action上限一致，避免恶意payload构造无界集合。 */
    private static final int MAX_TARGETS = 1024;
    /** 单candidate必须受row-log frame格式的16MiB上界约束。 */
    private static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;

    /** 按manifest ordinal严格递增的ADD INDEX目标。 */
    private final List<OnlineAlterIndexTarget> targets;

    /**
     * 冻结一个operation的全部ADD INDEX目标。
     *
     * @param targets 非空、ordinal严格递增且index identity唯一的目标集合
     */
    public MultiIndexAlterCandidateCodec(List<OnlineAlterIndexTarget> targets) {
        if (targets == null || targets.isEmpty() || targets.size() > MAX_TARGETS) {
            throw new DatabaseValidationException(
                    "multi-index candidate targets are empty or exceed manifest limit");
        }
        this.targets = List.copyOf(targets);
        int previous = -1;
        Set<Long> ids = new HashSet<>();
        for (OnlineAlterIndexTarget target : this.targets) {
            if (target == null || target.actionOrdinal() <= previous
                    || !ids.add(target.indexId())) {
                throw new DatabaseValidationException(
                        "multi-index targets must be unique and manifest ordered");
            }
            previous = target.actionOrdinal();
        }
    }

    /** INSERT需要向每个staged secondary最终确保after entry。 */
    @Override
    public Optional<byte[]> encodeInsert(LogicalRecord after) {
        if (after == null) {
            throw new DatabaseValidationException(
                    "multi-index INSERT candidate row must not be null");
        }
        return encodeTargets(target -> target.candidateCodec().encodeInsert(after));
    }

    /** UPDATE只包含physical key实际变化的目标，未变化目标不会扩大日志或force集合。 */
    @Override
    public Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after) {
        if (before == null || after == null) {
            throw new DatabaseValidationException(
                    "multi-index UPDATE candidate rows must not be null");
        }
        return encodeTargets(target -> target.candidateCodec().encodeUpdate(before, after));
    }

    /** DELETE需要向每个staged secondary最终删除before entry。 */
    @Override
    public Optional<byte[]> encodeDelete(LogicalRecord before) {
        if (before == null) {
            throw new DatabaseValidationException(
                    "multi-index DELETE candidate row must not be null");
        }
        return encodeTargets(target -> target.candidateCodec().encodeDelete(before));
    }

    /**
     * 严格解码完整payload；未知flags、重复/乱序ordinal、重复index或长度尾随均拒绝。
     *
     * @param payload 完整外层candidate bytes
     * @return 按manifest ordinal排序的不可变entry集合
     * @throws DatabaseValidationException 格式、数量、identity或长度不合法时抛出
     */
    @Override
    public OnlineAlterCandidate decode(byte[] payload) {
        if (payload == null || payload.length < PREFIX_BYTES
                || payload.length > MAX_PAYLOAD_BYTES) {
            throw new DatabaseValidationException(
                    "multi-index candidate payload length is invalid");
        }
        ByteBuffer input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int magic = input.getInt();
        short version = input.getShort();
        short flags = input.getShort();
        int count = input.getInt();
        if (magic != MAGIC || version != VERSION || flags != 0
                || count <= 0 || count > MAX_TARGETS) {
            throw new DatabaseValidationException(
                    "multi-index candidate header is invalid");
        }
        List<OnlineAlterCandidateEntry> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if (input.remaining() < ENTRY_PREFIX_BYTES) {
                throw new DatabaseValidationException(
                        "multi-index candidate entry prefix is truncated");
            }
            int ordinal = input.getInt();
            long indexId = input.getLong();
            int length = input.getInt();
            if (length <= 0 || length > input.remaining()) {
                throw new DatabaseValidationException(
                        "multi-index candidate entry length is invalid");
            }
            byte[] nested = new byte[length];
            input.get(nested);
            entries.add(new OnlineAlterCandidateEntry(ordinal, indexId, nested));
        }
        if (input.hasRemaining()) {
            throw new DatabaseValidationException(
                    "multi-index candidate has trailing bytes");
        }
        return new OnlineAlterCandidate(entries);
    }

    /** 逐目标执行纯投影，只把有变化的结果封装进一个frame payload。 */
    private Optional<byte[]> encodeTargets(TargetEncoder encoder) {
        List<OnlineAlterCandidateEntry> entries = new ArrayList<>();
        for (OnlineAlterIndexTarget target : targets) {
            encoder.encode(target).ifPresent(payload -> entries.add(
                    new OnlineAlterCandidateEntry(target.actionOrdinal(), target.indexId(), payload)));
        }
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        int size = PREFIX_BYTES;
        try {
            for (OnlineAlterCandidateEntry entry : entries) {
                size = Math.addExact(size, Math.addExact(ENTRY_PREFIX_BYTES,
                        entry.payload().length));
            }
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException(
                    "multi-index candidate payload length overflows", overflow);
        }
        if (size > MAX_PAYLOAD_BYTES) {
            throw new DatabaseValidationException(
                    "multi-index candidate exceeds row-log frame limit");
        }
        ByteBuffer output = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).putShort(VERSION).putShort((short) 0).putInt(entries.size());
        for (OnlineAlterCandidateEntry entry : entries) {
            byte[] nested = entry.payload();
            output.putInt(entry.actionOrdinal()).putLong(entry.indexId())
                    .putInt(nested.length).put(nested);
        }
        return Optional.of(output.array());
    }

    /** 允许INSERT/UPDATE/DELETE共享有序封装逻辑的纯函数适配。 */
    @FunctionalInterface
    private interface TargetEncoder {
        Optional<byte[]> encode(OnlineAlterIndexTarget target);
    }
}
