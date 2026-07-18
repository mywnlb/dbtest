/**
 * {@code cn.zhangyis.db.storage.trx} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：事务、MVCC 与锁包，维护事务状态、ReadView、行锁和死锁检测，不直接操作 BufferFrame 或裸文件。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.trx;
