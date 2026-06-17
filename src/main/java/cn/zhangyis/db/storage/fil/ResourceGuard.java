package cn.zhangyis.db.storage.fil;

/**
 * RAII 风格释放守卫。配合 try-with-resources 使用，{@link #close()} 收窄为不抛受检异常，
 * 保证锁/资源在作用域结束时确定性释放（AGENTS.md Guard 模式）。
 */
public interface ResourceGuard extends AutoCloseable {

    /**
     * 释放守卫持有的资源（如解锁）。不得抛受检异常，便于在 try-with-resources 中无样板地释放。
     */
    @Override
    void close();
}
