package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionarySnapshot;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 一个已经由 manifest journal 原子发布并完整校验的 clean 字典恢复快照。
 *
 * @param sequence 承载该快照的 manifest batch sequence
 * @param resolvedThroughSequence 本快照明确裁决的此前 manifest event 上界
 * @param dictionarySnapshot ACTIVE/DISCARDED 对象组成的稳定字典 baseline
 * @param controlSnapshot 发布时的安全 identity 高水位
 * @param paths ACTIVE 表的一一对应目录见证
 * @param digest clean event 完整逻辑 body 的 SHA-256
 */
public record DictionaryRecoveryCleanSnapshot(
        long sequence,
        long resolvedThroughSequence,
        DictionarySnapshot dictionarySnapshot,
        DictionaryControlSnapshot controlSnapshot,
        List<DictionaryRecoveryPathEntry> paths,
        byte[] digest) {

    /** SHA-256 的固定字节数。 */
    private static final int SHA256_BYTES = 32;

    /**
     * 校验序号边界并冻结 path/digest，确保已经发布的恢复证据不能被调用方事后修改。
     */
    public DictionaryRecoveryCleanSnapshot {
        if (sequence <= 0 || resolvedThroughSequence < 0 || resolvedThroughSequence >= sequence
                || dictionarySnapshot == null || controlSnapshot == null || paths == null
                || paths.stream().anyMatch(Objects::isNull)
                || digest == null || digest.length != SHA256_BYTES) {
            throw new DatabaseValidationException("dictionary recovery clean snapshot is invalid");
        }
        paths = List.copyOf(paths);
        digest = Arrays.copyOf(digest, digest.length);
    }

    /**
     * 返回独立摘要副本，避免离线 token 使用的 manifest identity 被外部修改。
     *
     * @return 长度恒为 32 的 clean body SHA-256 副本
     */
    @Override
    public byte[] digest() {
        return Arrays.copyOf(digest, digest.length);
    }

    /**
     * 按 clean body 摘要内容比较完整恢复快照。
     *
     * @param other 待比较对象
     * @return sequence、archive/control/path 与 digest 字节全部相同时为 {@code true}
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof DictionaryRecoveryCleanSnapshot that
                && sequence == that.sequence
                && resolvedThroughSequence == that.resolvedThroughSequence
                && dictionarySnapshot.equals(that.dictionarySnapshot)
                && controlSnapshot.equals(that.controlSnapshot)
                && paths.equals(that.paths)
                && Arrays.equals(digest, that.digest);
    }

    /**
     * 生成与内容相等语义一致的哈希值。
     *
     * @return clean snapshot 字段与摘要内容的组合哈希
     */
    @Override
    public int hashCode() {
        return 31 * Objects.hash(sequence, resolvedThroughSequence, dictionarySnapshot,
                controlSnapshot, paths) + Arrays.hashCode(digest);
    }
}
