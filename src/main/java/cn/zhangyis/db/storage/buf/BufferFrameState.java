package cn.zhangyis.db.storage.buf;

/**
 * Buffer Pool 帧生命周期 / IO 状态（设计 §5.7）。取代散落布尔表达"空闲 / 载入中 / 刷盘中"等阶段，
 * 转换由 {@link FrameStateMachine} 在目标 frame 的 frameMutex 下集中执行，避免多处直接改字段造成不一致。
 *
 * <p>与帧 {@code dirty} 布尔的关系：{@code dirty ⟺ state ∈ {DIRTY, FLUSHING}}。DIRTY_PENDING 仍由活跃 MTR
 * 独占，尚未进入 flush list；FLUSHING 期页仍 dirty（未 durable），
 * 仅 {@code completeFlush} 成功才清；故 checkpoint 的 oldest-dirty 边界仍计入 FLUSHING 帧。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code EVICTING}：表示“EVICTING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
enum BufferFrameState {

    /** 不绑定任何 page，位于 free list。 */
    FREE,

    /** 已认领并注册占位、正在读盘载入；内容未就绪，唯一 IO owner 持有，其它命中者等待 load future。 */
    LOADING,

    /** 绑定 page，内容与磁盘一致，位于 LRU。 */
    CLEAN,

    /** 当前 MTR 已修改但尚未发布 redo/pageLSN；不能刷盘或进入淘汰候选。 */
    DIRTY_PENDING,

    /** 绑定 page，存在未刷盘修改，不在刷盘中。 */
    DIRTY,

    /** dirty 页正在写出（仍可被读）；单 IO owner，淘汰/候选枚举跳过它，避免重复刷或被淘汰。 */
    FLUSHING

    /** 已从 hash/LRU 隔离、等待复用的淘汰候选。 */,
    EVICTING,

    /** 所属 tablespace 版本已失效；普通读路径不得返回该帧。 */
    STALE
}
