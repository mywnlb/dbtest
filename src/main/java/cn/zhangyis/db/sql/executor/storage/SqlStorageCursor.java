package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.executor.row.SqlRowView;

/**
 * SQL Data Port 的 pull-based 查询游标。实现必须在每次 {@link #advance()} 返回前释放
 * page latch、buffer fix 和 MTR；cursor close 负责 RC ReadView 与 adapter operation lease。
 */
public interface SqlStorageCursor extends AutoCloseable {

    /**
     * 拉取下一条已完成 MVCC/current-read 选择的完整逻辑行。
     *
     * @return 有新当前行时为 {@code true}，到达 EOF 时为 {@code false}
     */
    boolean advance();

    /**
     * 返回最近一次成功 advance 的 cursor-owned 行视图。
     *
     * @return 只在下一次 advance/close 前有效的逻辑行
     */
    SqlRowView current();

    /**
     * 幂等释放 cursor、RC ReadView 和 adapter operation lease；事务行锁不在此释放。
     */
    @Override
    void close();
}
