package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 操作上界无法装入 repository 允许的单个 sealed batch。该错误发生在任何页资源获取之前，说明领域操作必须拆批
 * 或修正估算/redo 文件配置，继续执行会在 append 时留下不可恢复的半批次风险。
 */
public final class RedoBudgetTooLargeException extends DatabaseFatalException {

    /** 创建包含用途、请求物理字节与 repository 上限的诊断。 */
    public RedoBudgetTooLargeException(String message) {
        super(message);
    }
}
