package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * Redo 文件结构损坏。表示 checkpoint 后的完整 redo record/block 无法可信解析，继续恢复会破坏页内容安全。
 */
public class RedoLogCorruptedException extends DatabaseFatalException {

    public RedoLogCorruptedException(String message) {
        super(message);
    }

    public RedoLogCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
