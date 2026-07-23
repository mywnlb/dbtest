/**
 * 对不可变 LogicalPlan 执行有界等价改写的规则框架。规则不得读取统计、存储内部状态或
 * 执行资源；矛盾证明、SARG 提取和访问路径选择属于后续独立阶段。
 */
package cn.zhangyis.db.sql.optimizer.rewrite;
