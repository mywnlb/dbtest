/**
 * {@code cn.zhangyis.db.session} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：连接与会话编排包，负责命令分发、autocommit 与 prepared statement 生命周期，只经稳定 SQL/存储入口下推请求。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.session;
