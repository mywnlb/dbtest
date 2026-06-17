/**
 * 数据库事务模块（storage.trx，innodb-transaction-mvcc-design §3-§5）。本片（T1.1）实现纯内存事务生命周期：
 * 事务 id/提交序号分配、事务状态机、活跃事务表、{@code TransactionManager} 门面。
 *
 * <p>边界：本模块不直接操作 {@code BufferFrame}/{@code PageGuard}/裸文件，不实现 B+Tree 查找；它只定义事务语义。
 * ReadView/可见性、undo、行锁、purge 留后续片（依赖顺序：隐藏列 → undo → 可见性）。
 *
 * <p>并发：{@code TransactionSystem} 用 {@link java.util.concurrent.locks.ReentrantLock} 短锁保护 id/no 分配与
 * 活跃表，拷贝快照后立即释放，持锁期间不做 IO、不持 page latch、不等待。禁止 {@code synchronized}。
 */
package cn.zhangyis.db.storage.trx;
