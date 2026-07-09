package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoRecord;

/**
 * MTR collector 内部的诊断条目：把真正会提交给 {@code RedoLogManager} 的 redo record 与 MTR 本地分类
 * 并排保存。该类型不参与 redo 编码，也不出现在恢复路径；恢复仍只依赖 {@link RedoRecord}。
 *
 * @param record   实际持久化的 redo record。
 * @param category MTR 本地分类，用于测试和后续 MLOG 迁移审计。
 */
record MtrRedoEntry(RedoRecord record, MtrRedoCategory category) {

    MtrRedoEntry {
        if (record == null || category == null) {
            throw new DatabaseValidationException("MTR redo entry record/category must not be null");
        }
    }
}
