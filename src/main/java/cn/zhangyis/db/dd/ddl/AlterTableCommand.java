package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;

import java.util.List;

/**
 * 一条通用阻塞式 ALTER 命令；actions 必须以用户声明顺序在同一个 staged definition 上求值。
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
