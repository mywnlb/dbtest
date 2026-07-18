/**
 * {@code cn.zhangyis.db.storage.recovery} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：崩溃恢复编排包，按 doublewrite 修复、redo 重放、未提交事务回滚与 purge 恢复顺序推进阶段。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.recovery;
