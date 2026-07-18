/**
 * {@code cn.zhangyis.db.storage.record} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：记录格式包，负责页内字段编码、隐藏列与 Page Directory，不分配物理页或持有长期事务锁。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.record;
