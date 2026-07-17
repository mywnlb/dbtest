package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DML 等待行物理协调 guard 时线程被中断；实现会恢复 interrupt flag，调用方应终止当前 statement。 */
public final class PurgeDmlRowGuardInterruptedException extends DatabaseRuntimeException {

    /**
     * 构造等待中断异常并保留底层原因。
     *
     * @param message 包含 table id、cluster key 等行协调上下文的诊断信息。
     * @param cause   触发失败的 {@link InterruptedException}；manager 在构造前已恢复线程 interrupt flag。
     */
    public PurgeDmlRowGuardInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
