package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * Calcite-Lite 单表物理计划根。M4 SELECT 使用不可变 PhysicalQuery 算子树，DML 使用
 * 原子 physical command；两者都不持有事务、ReadView、PlanNode、锁租约或 cursor。
 */
public sealed interface PhysicalPlan permits PhysicalInsert, PhysicalQuery,
        PhysicalJoinQuery,
        PhysicalPointUpdate, PhysicalPointDelete, PhysicalRangeUpdate,
        PhysicalRangeDelete {

    /**
     * 返回编译期间 metadata lease 固定的精确表版本。
     *
     * @return 后续 Executor 与 Data Port 必须共同使用的不可变表定义
     */
    default TableDefinition table() {
        List<TableDefinition> tables = tables();
        if (tables.size() != 1) {
            throw new DatabaseValidationException(
                    "multi-relation physical plan has no unique table");
        }
        return tables.getFirst();
    }

    /**
     * 返回 SQL 顺序排列的 exact table versions；既有单表计划由 table() 自动提升。
     *
     * @return 非空 immutable relation list
     */
    default List<TableDefinition> tables() {
        return List.of(table());
    }
}
