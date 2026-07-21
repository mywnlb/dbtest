package cn.zhangyis.db.dd.domain;

/** 字典层表生命周期；DROP_PENDING 只在 DDL/recovery 内可见，普通 lookup 只返回 ACTIVE。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code ACTIVE}：表示“ACTIVE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code DROP_PENDING}：表示“DROPPENDING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code DROPPED}：表示“DROPPED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code DISCARD_PENDING}：表示“DISCARDPENDING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code DISCARDED}：表示“DISCARDED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code IMPORT_PENDING}：表示“IMPORTPENDING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code RECOVERY_UNAVAILABLE}：强制恢复已把整个对象隔离，物理文件仍保留但不得进入普通发现、页访问或 undo/purge 物理修改</li>
 *     <li>{@code RECOVERY_DISCARDED}：隔离对象的原文件已受控移出 tables 目录，只能通过可信恢复备份重新激活</li>
 * </ul>
 */
public enum TableState {
    ACTIVE,
    DROP_PENDING,
    DROPPED,
    DISCARD_PENDING,
    DISCARDED,
    IMPORT_PENDING,
    RECOVERY_UNAVAILABLE,
    RECOVERY_DISCARDED
}
