package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.storage.record.page.SearchKey;

/**
 * Online shadow base copy 在完整批次释放 source page guard、且该批全部 target MTR 已提交后的观察接缝。
 * 上层可在此检查 durable cancellation、更新 tracker 或中止 copy；实现不得持有回调跨越下一批。
 */
@FunctionalInterface
public interface OnlineShadowCopyObserver {

    /** 不需要可观察性或取消的 blocking/低层调用使用的无副作用实例。 */
    OnlineShadowCopyObserver NO_OP = (rowsInBatch, continuation) -> { };

    /**
     * 通知一个非空批次已经完整落入未发布 shadow。
     *
     * @param rowsInBatch 当前批次完成投影与全部索引写入的正行数
     * @param continuation 下一批 exclusive scan 使用的完整聚簇物理键；不得为空
     */
    void onBatchCompleted(long rowsInBatch, SearchKey continuation);
}
