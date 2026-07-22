package cn.zhangyis.db.storage.trx;

/**
 * purge dispatcher 完成一个稳定批次后的维护扩展点。
 *
 * <p>调用发生在 worker token、row guard、history removal lease 与 MTR 全部释放之后，但仍属于同一 driver cycle；
 * 实现不得重新进入 purge worker pool，也不得把 maintenance 资源泄漏到下一批。</p>
 */
@FunctionalInterface
public interface PurgeCycleMaintenance {

    /**
     * 在成功批次后执行维护；零进展 summary 仍会调用，使独立的空间回收不依赖新 history 到达。
     *
     * @param summary 已完成 purge batch 的稳定统计；不得为 {@code null}，仅用于观测和策略判断
     */
    void afterSuccessfulBatch(PurgeSummary summary);
}
