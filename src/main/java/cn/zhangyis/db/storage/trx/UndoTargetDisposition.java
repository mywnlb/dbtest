package cn.zhangyis.db.storage.trx;

/** undo identity 对应 DD 对象的物理准入状态；编码解码仍可使用 metadata，但只有 AVAILABLE 可访问用户页。 */
public enum UndoTargetDisposition {
    /** 表对象允许普通 rollback/purge 访问全部 B+Tree 与 LOB。 */
    AVAILABLE,
    /** 表对象已被 FORCE 持久隔离，只允许 recovery 校验 undo 链并推进系统 undo logical head。 */
    RECOVERY_UNAVAILABLE
}
