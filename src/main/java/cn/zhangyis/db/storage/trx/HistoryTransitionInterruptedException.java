package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** history transition 等待被中断；构造前调用方已恢复线程中断标志，并保留 InterruptedException 根因。 */
public final class HistoryTransitionInterruptedException extends DatabaseRuntimeException {

    public HistoryTransitionInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
