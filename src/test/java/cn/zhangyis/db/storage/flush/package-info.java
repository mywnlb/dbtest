/**
 * {@code cn.zhangyis.db.storage.flush} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：刷脏、doublewrite 与 checkpoint 协作包，确保任何数据页写盘前其 page LSN 已满足 WAL。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.flush;
