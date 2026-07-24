package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.optimizer.physical.PhysicalAccess;

/**
 * 单条查询的 cursor 资源作用域。一个 scope 对应一个 transaction operation lease、
 * 一个绝对 deadline 和至多一个一致性 ReadView，可以同时拥有 outer/inner 多个 cursor。
 */
public interface SqlCursorScope extends AutoCloseable {

    /**
     * 在当前语句资源边界内打开一个访问叶 cursor。
     *
     * @param access Optimizer 产生或参数化 JOIN probe 实例化的访问叶
     * @return 已打开且归本 scope 管理的 cursor
     */
    SqlStorageCursor openCursor(
            PhysicalAccess access);

    /**
     * 关闭全部仍打开 cursor，再释放 RC ReadView 与 transaction operation lease；必须幂等。
     */
    @Override
    void close();
}
