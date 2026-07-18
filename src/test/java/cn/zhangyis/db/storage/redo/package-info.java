/**
 * {@code cn.zhangyis.db.storage.redo} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：Redo/WAL 包，维护 LSN、日志缓冲、writer/flusher 与幂等重放，不调用事务锁或具体 repository 实现。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.redo;
