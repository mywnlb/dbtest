package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Redo 文件环配置（0.18b）。在 {@link EngineConfig} 中为可空组件：为 {@code null} 表示沿用单 append-only redo 文件；
 * 非空表示启用 {@link cn.zhangyis.db.storage.redo.RotatingRedoLogRepository} 文件环，让长跑下 redo 占用有界。
 *
 * @param fileCount 文件环文件数；要求 ≥2，否则环满时无另一个文件可回收复用。
 * @param fileBytes 单文件帧容量上限（不含文件头）；必须大于引擎可能产生的最大单批 redo，否则该批 append 报配置错误。
 */
public record RedoRotationConfig(int fileCount, long fileBytes) {

    /** 默认文件数：教学实现取 8（InnoDB 量级偏小），与单文件容量配合给出有界但宽裕的 redo 上限。 */
    private static final int DEFAULT_FILE_COUNT = 8;
    /**
     * 默认单文件容量：8 MiB。必须远大于引擎任一单个 MTR 批次的 redo 字节数（boot 建库/索引操作均 ≪ 8 MiB），
     * 否则该批 append 会判为配置错误；同时 8×8 MiB=64 MiB 总上限对一般工作负载不会触发环满。
     */
    private static final long DEFAULT_FILE_BYTES = 8L * 1024 * 1024;

    public RedoRotationConfig {
        if (fileCount < 2) {
            throw new DatabaseValidationException("redo rotation fileCount must be >= 2: " + fileCount);
        }
        if (fileBytes <= 0) {
            throw new DatabaseValidationException("redo rotation fileBytes must be positive: " + fileBytes);
        }
    }

    /** 默认 redo 文件环配置（8 个文件、每文件 8 MiB），作为 {@link EngineConfig} 的默认 redo 后端。 */
    public static RedoRotationConfig defaults() {
        return new RedoRotationConfig(DEFAULT_FILE_COUNT, DEFAULT_FILE_BYTES);
    }
}
