/**
 * {@code cn.zhangyis.db.storage.undo} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：Undo 与 purge 输入包，记录回滚所需的逻辑历史并支持事务回退，不绕过事务模块决定可见性。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.undo;
