package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;

import java.time.Duration;

/**
 * Commit 刷盘策略（§8）。把「事务提交时对 redo 持久性的要求」抽象为策略：commit 层据此选用 redo 的
 * write/flush/wait 原语，redo 模块本身不决定事务是否对外提交成功。
 *
 * <p>三阶段语义：<b>append</b>（分配 LSN、写内存 log buffer）→ <b>write</b>（写到 OS page cache，进程崩溃不丢、
 * 但宕机/断电可能丢）→ <b>flush</b>（fsync 到磁盘，真正 durable）。各策略在 commit 处停在不同阶段，换取持久性 vs 延迟。
 *
 * <p>简化点：生产 {@code TransactionManager.commit}/DML facade 接线属 2.1，本枚举目前由测试与未来 commit 编排消费；
 * {@code SYNC_EVERY_N_MS} 暂不单列，等价于 {@link #BACKGROUND_FLUSH} + 现有周期性 {@link RedoFlushWorker}。
 */
public enum DurabilityPolicy {

    /** commit 等待 redo fsync 到磁盘（默认强 ACID）：宕机不丢已提交事务。 */
    FLUSH_ON_COMMIT {
        @Override
        public boolean awaitCommitDurable(RedoLogManager redo, Lsn commitLsn, Duration timeout) {
            redo.flush();
            return redo.waitFlushed(commitLsn, timeout);
        }
    },

    /** commit 等待 redo 写到 OS page cache、不等 fsync：延迟更低，但宕机/断电可能丢最近已提交事务。 */
    WRITE_ON_COMMIT {
        @Override
        public boolean awaitCommitDurable(RedoLogManager redo, Lsn commitLsn, Duration timeout) {
            redo.write();
            return redo.waitWritten(commitLsn, timeout);
        }
    },

    /** commit 不等待 redo 落盘，立即返回，由后台 {@link RedoFlushWorker} 周期写/刷：测试或低可靠模式，崩溃可能丢。 */
    BACKGROUND_FLUSH {
        @Override
        public boolean awaitCommitDurable(RedoLogManager redo, Lsn commitLsn, Duration timeout) {
            // 刻意不等待：持久性由后台 flusher 最终保证，commit 立即返回。
            return true;
        }
    };

    /**
     * 按本策略在事务提交点等待 redo 达到要求的持久阶段。
     *
     * @param redo      redo 管理器（提供 write/flush/wait 原语）。
     * @param commitLsn 本次提交需要覆盖到的 redo LSN（通常为提交批次的 endLsn）。
     * @param timeout   等待上限；不无界等待。
     * @return 是否在超时内达到本策略要求的持久阶段（{@link #BACKGROUND_FLUSH} 恒 true）。
     */
    public abstract boolean awaitCommitDurable(RedoLogManager redo, Lsn commitLsn, Duration timeout);
}
