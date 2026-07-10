package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * Redo 持久格式不受当前引擎支持。它与随机 checksum 损坏分开表达，便于运维明确判断是需要离线重建，
 * 还是介质/写入损坏；两者都不能继续普通启动。
 */
public final class RedoLogFormatException extends DatabaseFatalException {

    /** 创建带格式诊断信息的致命异常。 */
    public RedoLogFormatException(String message) {
        super(message);
    }

    /** 创建并保留底层解析根因的致命异常。 */
    public RedoLogFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
