package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;

import java.util.List;

/**
 * 一条通用 ALTER 命令；coordinator先按action形状选择instant/in-place或blocking策略，未被在线策略
 * 接受的actions必须以用户声明顺序在同一个staged definition上求值。
 *
 * @param table 语句开始时的逻辑表名
 * @param actions 非空有序 action
 */
public record AlterTableCommand(QualifiedTableName table, List<AlterTableAction> actions) {
    public AlterTableCommand {
        if (table == null || actions == null || actions.isEmpty()
                || actions.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "ALTER TABLE command requires table and non-empty actions");
        }
        actions = List.copyOf(actions);
    }
}
