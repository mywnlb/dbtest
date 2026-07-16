package cn.zhangyis.db.dd.mdl;

/** metadata lock 的释放作用域；session 层后续按 statement/transaction 结束调用 releaseAll。 */
public enum MdlDuration {
    STATEMENT,
    TRANSACTION,
    EXPLICIT
}
