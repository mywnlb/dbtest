package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.dd.domain.TableDefinition;

/**
 * M1 不可变逻辑关系节点。节点只表达 SQL 关系语义和 exact metadata binding，不包含访问索引、B+Tree
 * range、存储句柄或执行期资源。
 */
public sealed interface RelNode permits LogicalTableScan, LogicalFilter, LogicalProject,
        LogicalValues, LogicalTableModify {

    /**
     * 返回当前单表关系树绑定的 exact table version。
     *
     * @return statement metadata lease 固定且不为 {@code null} 的表定义
     */
    TableDefinition table();
}
