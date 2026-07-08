package cn.zhangyis.db.storage.redo;

/**
 * 一个 redo batch 内某类 handler 的有状态回放会话。
 *
 * <p>MTR 的物理恢复语义要求同一 batch 内的记录按文件顺序进入 handler，但某些 handler（例如页物理
 * handler）必须延迟到 batch 末尾统一发布 pageLSN。因此 dispatcher 不能只暴露单条 record 回调，
 * 而是为每个参与本 batch 的 handler 打开一个 session，顺序调用 {@link #apply(RedoRecord)}，最后调用
 * {@link #finish()} 提交该 handler 的批末副作用。
 */
public interface RedoApplyBatchHandler {

    /**
     * 按 redo 文件中的原始顺序应用一条 record。实现可以暂存修改，等 {@link #finish()} 再写出。
     *
     * @param record 已由 owning {@link RedoApplyHandler} 声明支持的 redo record。
     */
    void apply(RedoRecord record);

    /**
     * 完成本 handler 在当前 batch 的批末动作，例如统一盖 pageLSN 并写回物理页。
     */
    void finish();
}
