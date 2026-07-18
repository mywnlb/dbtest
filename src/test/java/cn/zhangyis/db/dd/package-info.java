/**
 * {@code cn.zhangyis.db.dd} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：数据字典包，维护 schema、table、index 元数据及 DDL 状态，不直接管理物理页或 BufferFrame。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.dd;
