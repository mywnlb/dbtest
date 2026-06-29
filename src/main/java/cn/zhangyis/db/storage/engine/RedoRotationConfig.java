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

    public RedoRotationConfig {
        if (fileCount < 2) {
            throw new DatabaseValidationException("redo rotation fileCount must be >= 2: " + fileCount);
        }
        if (fileBytes <= 0) {
            throw new DatabaseValidationException("redo rotation fileBytes must be positive: " + fileBytes);
        }
    }
}
