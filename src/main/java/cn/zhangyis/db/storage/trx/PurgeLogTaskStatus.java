package cn.zhangyis.db.storage.trx;

/** 一条 history log 在本批 worker 阶段结束时的稳定状态。 */
enum PurgeLogTaskStatus {
    /** 持久 logical head 已为空，可以由 dispatcher 按物理 head 顺序 finalization。 */
    READY,
    /** 当前 record 的 DML row guard busy；未越过该 record 的持久进度边界。 */
    DEFERRED,
    /** 同表前驱未 READY 或 eligibility 复核失败，本条没有执行物理任务。 */
    BLOCKED,
    /** 执行发生领域异常；已提交的短 MTR 保持幂等证据，错误由 dispatcher 汇总抛出。 */
    FAILED
}
