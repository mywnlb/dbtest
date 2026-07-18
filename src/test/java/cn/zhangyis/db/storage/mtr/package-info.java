/**
 * {@code cn.zhangyis.db.storage.mtr} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：Mini Transaction 包，以短物理临界区管理 latch、fix memo 与 redo 收集，不承担数据库事务提交语义。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.mtr;
