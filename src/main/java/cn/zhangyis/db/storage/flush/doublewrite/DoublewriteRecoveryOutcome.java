package cn.zhangyis.db.storage.flush.doublewrite;

/**
 * doublewrite recovery 对单页检查的结果。它区分“已修复”和“只检测到可疑页”，避免统计层把 detect-only
 * metadata 误计为已修复页。
 */
public enum DoublewriteRecoveryOutcome {
    /** data page checksum 有效，或 doublewrite 文件没有覆盖该页。 */
    CLEAN_OR_NOT_COVERED,
    /** 使用 full-copy slot 写回并 force 了 data file。 */
    REPAIRED_FROM_COPY,
    /** data page checksum 无效且只命中 detect-only metadata，未写回 data file。 */
    DETECTED_ONLY
}
