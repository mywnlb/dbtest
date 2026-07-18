package cn.zhangyis.db.storage.api.undotruncate;

/** undo 截断显式故障边界；测试用它覆盖 marker、物理缩短、重建、最终发布间的 crash 续作。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code AFTER_MARKER_DURABLE}：表示“之后MARKER持久”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code AFTER_BUFFER_INVALIDATION}：表示“之后BUFFERINVALIDATION”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code AFTER_PHYSICAL_TRUNCATE}：表示“之后PHYSICALTRUNCATE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code AFTER_REBUILD_DURABLE}：表示“之后REBUILD持久”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code AFTER_FINAL_STATE_DURABLE}：表示“之后FINAL状态持久”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum UndoTruncationPhase {
    AFTER_MARKER_DURABLE,
    AFTER_BUFFER_INVALIDATION,
    AFTER_PHYSICAL_TRUNCATE,
    AFTER_REBUILD_DURABLE,
    AFTER_FINAL_STATE_DURABLE
}
