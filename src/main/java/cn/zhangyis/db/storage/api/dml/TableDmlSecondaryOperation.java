package cn.zhangyis.db.storage.api.dml;

/** 故障诊断中的 secondary 物理操作类型。 */
enum TableDmlSecondaryOperation {
    /** 发布新 entry，或把同一物理 identity 从 delete-marked 恢复为 live。 */
    INSERT_OR_REVIVE,
    /** 只翻转已有 entry 的 delete 标记，不执行物理删除。 */
    DELETE_MARK
}
