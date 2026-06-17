package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Redo 文件 IO 异常。表示 redo 文件打开、追加、force 或扫描时发生可诊断的运行时错误；
 * 调用方可关闭实例、重试启动恢复或向上报告，但不能把底层 IOException 丢失。
 */
public class RedoLogIoException extends DatabaseRuntimeException {

    public RedoLogIoException(String message, Throwable cause) {
        super(message, cause);
    }
}
