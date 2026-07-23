package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.dd.domain.TableDefinition;

/**
 * M1 单表物理计划根。物理计划固定访问路径和执行语义，但不持有事务、ReadView、锁租约、
 * B+Tree cursor 或其它运行期资源。
 */
public sealed interface PhysicalPlan permits PhysicalInsert, PhysicalPointSelect,
        PhysicalSecondaryRangeSelect, PhysicalRangeSelect, PhysicalPointUpdate,
        PhysicalPointDelete, PhysicalRangeUpdate, PhysicalRangeDelete {

    /**
     * 返回编译期间 metadata lease 固定的精确表版本。
     *
     * @return 后续 Executor 与 Data Port 必须共同使用的不可变表定义
     */
    TableDefinition table();
}
