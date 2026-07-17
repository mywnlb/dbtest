package cn.zhangyis.db.storage.trx;

/** Purge task commit 后的包内测试故障接缝；生产默认 no-op。 */
@FunctionalInterface
interface PurgeProgressFaultInjector {

    /** 生产默认实现。 */
    PurgeProgressFaultInjector NO_OP = (phase, indexId) -> { };

    /**
     * 在单索引 purge MTR 已提交、history 尚未 finalization 的稳定边界通知测试；实现可抛异常模拟 crash。
     *
     * @param phase   已完成的 secondary 或 clustered 物理任务类型；对应页修改与 redo/dirty 已发布。
     * @param indexId 已提交 MTR 对应的稳定索引 id，用于多索引任务中精确定位故障步骤。
     */
    void onBoundary(PurgeProgressPhase phase, long indexId);
}
