package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * {@link RedoFlushTarget} 的生产适配器：把后台 redo flusher 的驱动端口委托给 {@link RedoLogManager} 的
 * {@code currentLsn()} / {@code flushedToDiskLsn()} / {@code flush()}。只做转发，不引入刷盘策略或锁拆分。
 */
public final class RedoLogManagerFlushTarget implements RedoFlushTarget {

    private final RedoLogManager redo;

    public RedoLogManagerFlushTarget(RedoLogManager redo) {
        if (redo == null) {
            throw new DatabaseValidationException("redo log manager must not be null");
        }
        this.redo = redo;
    }

    @Override
    public Lsn currentLsn() {
        return redo.currentLsn();
    }

    @Override
    public Lsn flushedToDiskLsn() {
        return redo.flushedToDiskLsn();
    }

    @Override
    public Lsn flush() {
        return redo.flush();
    }
}
