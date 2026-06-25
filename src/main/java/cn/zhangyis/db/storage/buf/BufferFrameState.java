package cn.zhangyis.db.storage.buf;

/**
 * Buffer Pool 帧生命周期 / IO 状态（设计 §5.7）。取代散落布尔表达"空闲 / 载入中 / 刷盘中"等阶段，
 * 转换由 {@link FrameStateMachine} 在 poolLock 下集中执行，避免多处直接改字段造成不一致。
 *
 * <p>与帧 {@code dirty} 布尔的关系：{@code dirty ⟺ state ∈ {DIRTY, FLUSHING}}。FLUSHING 期页仍 dirty（未 durable），
 * 仅 {@code completeFlush} 成功才清；故 checkpoint 的 oldest-dirty 边界仍计入 FLUSHING 帧。
 */
enum BufferFrameState {

    /** 不绑定任何 page，位于 free list。 */
    FREE,

    /** 已认领并注册占位、正在读盘载入；内容未就绪，唯一 IO owner 持有，其它命中者等待 load future。 */
    LOADING,

    /** 绑定 page，内容与磁盘一致，位于 LRU。 */
    CLEAN,

    /** 绑定 page，存在未刷盘修改，不在刷盘中。 */
    DIRTY,

    /** dirty 页正在写出（仍可被读）；单 IO owner，淘汰/候选枚举跳过它，避免重复刷或被淘汰。 */
    FLUSHING
}
