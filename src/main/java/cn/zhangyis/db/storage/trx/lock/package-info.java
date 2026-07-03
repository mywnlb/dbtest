/**
 * 事务锁内核（storage.trx.lock）。本包实现内存级 record/gap/next-key/insert-intention 锁、等待队列、
 * timeout、row-lock wait-for graph 和轻量快照；不直接访问 BufferFrame、FileChannel、B+Tree page 或 SQL/session。
 *
 * <p>简化点：锁状态不持久化，deadlock victim 只抛异常不自动 rollback，B+Tree current-read 等待前释放 latch 与等待后
 * 重定位由后续切片接入。
 */
package cn.zhangyis.db.storage.trx.lock;
