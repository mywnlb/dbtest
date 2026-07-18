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
     * @throws BufferPoolStalePageException 页固定、闩锁、淘汰或 frame 代际校验失败时抛出；调用方应释放已持 Guard 后重试或终止操作
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

    /** 当前版本快照；用于判断已驻留 frame 是否仍属于当前生命周期。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code current} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    TablespaceVersion current(SpaceId spaceId) {
        lock.lock();
        try {
            return entryUnderLock(spaceId).version;
        } finally {
            lock.unlock();
        }
    }

    /** 当前版本且不在维护窗口时返回 true，供 loader 发布阶段复核。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param version 表空间文件或 segment 的稳定身份与生命周期快照；不得为 {@code null}，必须与已打开文件和当前 generation 一致
     * @return {@code isCurrentAndOpen} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
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

    /** 开始 invalidate：阻止此后新的普通 admission，但不推进版本。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws BufferPoolStalePageException 页固定、闩锁、淘汰或 frame 代际校验失败时抛出；调用方应释放已持 Guard 后重试或终止操作
     */
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

    /** drain+clean 全部成功后推进版本；维护窗口仍保持关闭，直到旧 frame 全部分片移除。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** invalidate 完成，重新开放 admission。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     */
    void finishInvalidation(SpaceId spaceId) {
        lock.lock();
        try {
            entryUnderLock(spaceId).invalidating = false;
        } finally {
            lock.unlock();
        }
    }

    /** invalidate 在 drain/clean 阶段失败，关闭维护窗口且不推进版本。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     */
    void abortInvalidation(SpaceId spaceId) {
        lock.lock();
        try {
            entryUnderLock(spaceId).invalidating = false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 按Buffer Pool并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code entryUnderLock} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private Entry entryUnderLock(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space lifecycle clock space id must not be null");
        }
        return entries.computeIfAbsent(spaceId, ignored -> new Entry());
    }

    /** 单表空间的内存态版本与维护窗口标志，字段只在 clock.lock 下访问。 */
    private static final class Entry {
        /**
         * 构造或发布时固定的 {@code version} 版本、文件身份或源位置；必须来自当前对象上下文，诊断、并发校验和恢复路径依赖其稳定性。
         */
        private TablespaceVersion version = TablespaceVersion.INITIAL;
        /**
         * 记录 {@code invalidating} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean invalidating;
    }
}
