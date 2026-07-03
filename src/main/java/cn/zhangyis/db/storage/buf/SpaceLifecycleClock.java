package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffer Pool 内存态表空间生命周期时钟。
 *
 * <p>Disk Manager/UNDO truncate 已经通过 {@code TablespaceAccessController} 的 X lease 阻止经 MTR 的普通访问，但
 * Buffer Pool 本身仍可能被测试、read-ahead、warmup 或未来控制面直接调用。本时钟给 Buffer Pool 自己提供第二道防线：
 * invalidate 开始后拒绝新 frame admission；drain 成功后推进版本；全部旧 frame 移除后才重新开放 admission。
 *
 * <p><b>并发</b>：内部用独立 {@link ReentrantLock} 保护每个 {@link SpaceId} 的版本和 invalidating 标志。该锁只在
 * 本对象方法内短持有，绝不跨 {@code BufferPoolInstance} 内部锁调用外部代码；因此不会形成 clock→instance→clock
 * 的长锁环。已经进入 admission 的线程可能与 begin 并发，随后会以旧版本 frame 参与 invalidate drain。
 */
final class SpaceLifecycleClock {

    /** 所有表空间版本状态的短临界区锁；不保护 frame/hash/LRU。 */
    private final ReentrantLock lock = new ReentrantLock();

    /** SpaceId -> 进程内生命周期版本；未出现过的空间按 INITIAL 处理。 */
    private final Map<SpaceId, Entry> entries = new HashMap<>();

    /**
     * 前台 page fix/create 的 admission 入口。若表空间正在 invalidate，普通路径必须失败并由上层重试或等待维护完成。
     *
     * @param spaceId 目标表空间。
     * @return 当前可绑定到新 frame 的版本。
     */
    TablespaceVersion admitForeground(SpaceId spaceId) {
        lock.lock();
        try {
            Entry entry = entryUnderLock(spaceId);
            if (entry.invalidating) {
                throw new BufferPoolStalePageException("tablespace is being invalidated: " + spaceId.value());
            }
            return entry.version;
        } finally {
            lock.unlock();
        }
    }

    /**
     * read-ahead/warmup 等后台预取路径的 admission 入口。维护窗口内返回 empty 语义（null），调用方应静默跳过。
     *
     * @param spaceId 目标表空间。
     * @return 当前版本；维护窗口内返回 null。
     */
    TablespaceVersion admitPrefetchOrNull(SpaceId spaceId) {
        lock.lock();
        try {
            Entry entry = entryUnderLock(spaceId);
            return entry.invalidating ? null : entry.version;
        } finally {
            lock.unlock();
        }
    }

    /** 当前版本快照；用于判断已驻留 frame 是否仍属于当前生命周期。 */
    TablespaceVersion current(SpaceId spaceId) {
        lock.lock();
        try {
            return entryUnderLock(spaceId).version;
        } finally {
            lock.unlock();
        }
    }

    /** 当前版本且不在维护窗口时返回 true，供 loader 发布阶段复核。 */
    boolean isCurrentAndOpen(SpaceId spaceId, TablespaceVersion version) {
        if (version == null) {
            return false;
        }
        lock.lock();
        try {
            Entry entry = entryUnderLock(spaceId);
            return !entry.invalidating && entry.version.equals(version);
        } finally {
            lock.unlock();
        }
    }

    /** 开始 invalidate：阻止此后新的普通 admission，但不推进版本。 */
    void beginInvalidation(SpaceId spaceId) {
        lock.lock();
        try {
            Entry entry = entryUnderLock(spaceId);
            if (entry.invalidating) {
                throw new BufferPoolStalePageException("tablespace invalidation already in progress: "
                        + spaceId.value());
            }
            entry.invalidating = true;
        } finally {
            lock.unlock();
        }
    }

    /** drain+clean 全部成功后推进版本；维护窗口仍保持关闭，直到旧 frame 全部分片移除。 */
    void advanceInvalidation(SpaceId spaceId) {
        lock.lock();
        try {
            Entry entry = entryUnderLock(spaceId);
            if (!entry.invalidating) {
                throw new DatabaseValidationException("tablespace invalidation has not begun: " + spaceId.value());
            }
            entry.version = entry.version.next();
        } finally {
            lock.unlock();
        }
    }

    /** invalidate 完成，重新开放 admission。 */
    void finishInvalidation(SpaceId spaceId) {
        lock.lock();
        try {
            entryUnderLock(spaceId).invalidating = false;
        } finally {
            lock.unlock();
        }
    }

    /** invalidate 在 drain/clean 阶段失败，关闭维护窗口且不推进版本。 */
    void abortInvalidation(SpaceId spaceId) {
        lock.lock();
        try {
            entryUnderLock(spaceId).invalidating = false;
        } finally {
            lock.unlock();
        }
    }

    private Entry entryUnderLock(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space lifecycle clock space id must not be null");
        }
        return entries.computeIfAbsent(spaceId, ignored -> new Entry());
    }

    /** 单表空间的内存态版本与维护窗口标志，字段只在 clock.lock 下访问。 */
    private static final class Entry {
        private TablespaceVersion version = TablespaceVersion.INITIAL;
        private boolean invalidating;
    }
}
