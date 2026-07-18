/**
 * {@code cn.zhangyis.db.storage.fsp} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：空间管理包，负责 space、extent、segment 与 page 的分配释放，不直接操作 FileChannel 或解析记录。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.fsp;
