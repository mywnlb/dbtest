package cn.zhangyis.db.storage.trx;

/** version-chain safety check 对一个 delete-marked secondary physical identity 的处理决定。 */
public enum SecondaryPurgeDecision {

    /** 当前版本到目标 undo 之间没有任何较新 live 版本需要该 identity，可以执行精确物理删除。 */
    REMOVE,

    /** 至少一个较新 live 版本仍映射到该 identity；本条 history 可完成，但 entry 必须保留。 */
    RETAIN
}
