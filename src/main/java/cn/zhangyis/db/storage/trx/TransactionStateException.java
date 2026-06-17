package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 事务状态/生命周期非法操作异常：非法状态转换、对只读事务分配写 id、对已结束（COMMITTED/ROLLED_BACK）
 * 或提交中事务再发起操作。调用方可据此回滚或上报，不应静默吞掉。
 */
public class TransactionStateException extends DatabaseRuntimeException {

    public TransactionStateException(String message) {
        super(message);
    }

    public TransactionStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
