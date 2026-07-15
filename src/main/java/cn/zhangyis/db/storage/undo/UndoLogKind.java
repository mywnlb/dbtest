package cn.zhangyis.db.storage.undo;

/**
 * undo log 种类（对齐 InnoDB insert/update/temporary undo）。ordinal 落盘到 undo page header UNDO_KIND 字段，
 * 顺序不可改（{@code UndoRecordCodecTest} 钉死）。v2 把 kind 复制到 first/chain 每张普通 UNDO 页；TEMPORARY
 * 仍未接入普通 undo tablespace。
 */
public enum UndoLogKind {
    /** insert undo：只服务事务回滚，提交后即可释放，不进 history list。 */
    INSERT,
    /** update undo：服务 rollback 与 MVCC 旧版本，需 purge 边界推进后释放（T1.3d）。 */
    UPDATE,
    /** 临时对象 undo（临时表模块接入后启用）。 */
    TEMPORARY
}
