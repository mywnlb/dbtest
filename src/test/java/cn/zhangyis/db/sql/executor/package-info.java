/**
 * {@code cn.zhangyis.db.sql.executor} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：SQL 执行包，以算子生命周期驱动物理计划并生成结果集，只通过 storage API 访问数据。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.sql.executor;
