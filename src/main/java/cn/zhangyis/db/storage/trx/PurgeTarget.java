package cn.zhangyis.db.storage.trx;

/**
 * 后台 purge driver 的最小驱动端口（0.4）。{@link PurgeDriverWorker} 只依赖 {@link #runBatch}，便于单测注入 fake
 * 验证周期/失败语义，而不搭建完整 purge 物理栈。生产实现是 {@link PurgeCoordinator}（其 {@code runBatch} 签名即此）。
 */
public interface PurgeTarget {

    /**
     * 同步处理一批已提交 update/delete undo（按 purge boundary 处理至多 {@code maxLogs} 条 committed log）。
     *
     * @param maxLogs 本批最多处理的 committed undo log 数（正）。
     * @return 本批统计。
     */
    PurgeSummary runBatch(int maxLogs);
}
