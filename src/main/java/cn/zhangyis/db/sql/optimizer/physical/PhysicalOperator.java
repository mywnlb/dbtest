package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.dd.domain.TableDefinition;

/**
 * SELECT 物理算子树中的不可变节点。节点只描述执行意图和 exact metadata，不持有事务、
 * ReadView、StorageCursor、行锁或其它运行期资源。
 */
public sealed interface PhysicalOperator permits PhysicalAccess, PhysicalProject,
        PhysicalFilter {

    /**
     * 返回当前算子所属的 exact DD table version。
     *
     * @return metadata lease 固定的不可变表定义；同一物理树中的所有节点必须返回同一实例
     */
    TableDefinition table();
}
