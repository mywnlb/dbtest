/**
 * {@code cn.zhangyis.db.storage.buf} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：缓冲池包，维护 frame、fix、页闩、替换与脏页状态，不解析 record、segment 或 SQL 语义。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.buf;
