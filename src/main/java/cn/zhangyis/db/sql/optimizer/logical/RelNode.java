package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * M1 不可变逻辑关系节点。节点只表达 SQL 关系语义和 exact metadata binding，不包含访问索引、B+Tree
 * range、存储句柄或执行期资源。
 */
public sealed interface RelNode permits LogicalTableScan, LogicalFilter, LogicalProject,
        LogicalValues, LogicalTableModify, LogicalJoin {

    /**
     * 返回当前单表关系树绑定的 exact table version。
     *
     * @return statement metadata lease 固定且不为 {@code null} 的表定义
     */
    default TableDefinition table() {
        List<TableDefinition> tables = tables();
        if (tables.size() != 1) {
            throw new DatabaseValidationException(
                    "multi-relation node has no unique table");
        }
        return tables.getFirst();
    }

    /**
     * 返回 SQL 顺序排列的 exact relation versions；单表节点由既有 table() 自动提升。
     *
     * @return 非空、不可变的 statement relation schema
     */
    default List<TableDefinition> tables() {
        return List.of(table());
    }
}
