package cn.zhangyis.db.dd.cache;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.exception.DictionaryCacheLoadException;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.exception.DictionaryVersionConflictException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 版本化 table dictionary cache。lock 只保护 map、pin count 和 LOADING future 注册；repository loader、future
 * completion callbacks 和任何外部 IO 都在锁外执行。旧 pinned version 可与新 current version 并存，最后一个 pin
 * 释放时才从 versions 回收。
 */
public final class DictionaryObjectCache {

    /** cache map/pin count 的唯一 owner；不能跨 loader 或外部回调。 */
    private final ReentrantLock lock = new ReentrantLock();

    /** DROP 等待 pin 归零的 condition；谓词始终在 lock 内重查。 */
    private final Condition pinsChanged = lock.newCondition();

    /** 每个 table 的全部仍驻留版本。 */
    private final Map<TableId, NavigableMap<Long, Entry>> versions = new HashMap<>();

    /** 普通 lookup 只读取的最新已发布版本。 */
    private final Map<TableId, Entry> current = new HashMap<>();

    /** 每个 table 至多一个 cache-miss loader。 */
    private final Map<TableId, CompletableFuture<TableDefinition>> loading = new HashMap<>();

    /**
     * DROP 的本地准入屏障版本。catalog append 的 durable 结果可能不确定，故该屏障必须阻止旧 repository
     * snapshot 把较早 ACTIVE 版本重新装入；只有严格更新的 ACTIVE 版本发布才能解除。
     */
    private final Map<TableId, DictionaryVersion> invalidatedAt = new HashMap<>();

    /** 可淘汰 current 版本上限；pinned/stale 版本允许临时使总数超出。 */
    private final int capacity;

    /** 只在 lock 内递增，用于近似 LRU。 */
    private long accessClock;

    public DictionaryObjectCache(int capacity) {
        if (capacity <= 0) {
            throw new DatabaseValidationException("dictionary cache capacity must be positive");
        }
        this.capacity = capacity;
    }

    /**
     * pin 最新 table 版本。leader 在锁外 load，followers 对同一 future 做有界等待；load 成功后所有线程回到
     * lock 内重新 pin current，避免 publish/invalidate 竞态下返回已不再 current 的裸对象。
     */
    public DictionaryPin<TableDefinition> pinTable(TableId tableId, Duration timeout,
                                                    DictionaryLoader<TableDefinition> loader) {
        validatePinArguments(tableId, timeout, loader);
        long timeoutNanos = timeoutNanos(timeout);
        long deadline = deadline(timeoutNanos);
        while (true) {
            CompletableFuture<TableDefinition> future;
            boolean leader = false;
            lock.lock();
            try {
                DictionaryVersion barrier = invalidatedAt.get(tableId);
                if (barrier != null) {
                    throw new DictionaryObjectNotFoundException("dictionary table is behind DROP barrier: table="
                            + tableId.value() + " version=" + barrier.value());
                }
                Entry hit = current.get(tableId);
                if (hit != null) {
                    return pin(hit);
                }
                future = loading.get(tableId);
                if (future == null) {
                    future = new CompletableFuture<>();
                    loading.put(tableId, future);
                    leader = true;
                }
            } finally {
                lock.unlock();
            }

            if (leader) {
                completeLoad(tableId, loader, future);
            }
            awaitLoad(tableId, future, remaining(deadline));
        }
    }

    /** 发布新 current version；旧 current 只被标 stale，pin owner 仍可安全读完。 */
    public void publishTable(TableDefinition table) {
        if (table == null) {
            throw new DatabaseValidationException("published table definition must not be null");
        }
        lock.lock();
        try {
            publishUnderLock(table);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 建立 DROP 本地准入屏障：移除 current 并把所有版本标 stale；已有 pin 仍持不可变对象直到 close。
     * 屏障可早于 catalog publish 建立，因为提交结果不确定时宁可要求重启裁决，也不能重载旧 ACTIVE 快照。
     */
    public void invalidateTable(TableId tableId, DictionaryVersion dropVersion) {
        if (tableId == null || dropVersion == null) {
            throw new DatabaseValidationException("cache invalidation table/version must not be null");
        }
        lock.lock();
        try {
            DictionaryVersion existingBarrier = invalidatedAt.get(tableId);
            if (existingBarrier != null && dropVersion.compareTo(existingBarrier) < 0) {
                throw new DictionaryVersionConflictException("cannot move dictionary DROP barrier backwards");
            }
            Entry entry = current.remove(tableId);
            if (entry != null && dropVersion.compareTo(entry.value.version()) <= 0) {
                current.put(tableId, entry);
                throw new DictionaryVersionConflictException("drop version must be newer than cached table version");
            }
            invalidatedAt.put(tableId, dropVersion);
            NavigableMap<Long, Entry> tableVersions = versions.get(tableId);
            if (tableVersions != null) {
                for (Entry version : tableVersions.values()) {
                    version.stale = true;
                }
                removeReleasedStale(tableId, tableVersions);
            }
            pinsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** DROP 在持有 MDL X 后有界等待所有历史 pin 释放；timeout 返回 false，不吞掉资源泄漏。 */
    public boolean awaitUnpinned(TableId tableId, Duration timeout) {
        if (tableId == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("await-unpinned table/positive timeout required");
        }
        long remaining = timeoutNanos(timeout);
        lock.lock();
        try {
            while (hasPins(tableId)) {
                if (remaining <= 0) {
                    return false;
                }
                try {
                    remaining = pinsChanged.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public DictionaryCacheSnapshot snapshot() {
        lock.lock();
        try {
            int versionCount = versions.values().stream().mapToInt(Map::size).sum();
            int pinned = versions.values().stream().flatMap(map -> map.values().stream())
                    .mapToInt(entry -> entry.pinCount > 0 ? 1 : 0).sum();
            return new DictionaryCacheSnapshot(current.size(), versionCount, pinned, loading.size());
        } finally {
            lock.unlock();
        }
    }

    private void completeLoad(TableId tableId, DictionaryLoader<TableDefinition> loader,
                              CompletableFuture<TableDefinition> future) {
        TableDefinition loaded = null;
        Throwable failure = null;
        try {
            loaded = loader.load().orElseThrow(() -> new DictionaryObjectNotFoundException(
                    "dictionary table not found: " + tableId.value()));
            if (!loaded.id().equals(tableId)) {
                throw new DictionaryCacheLoadException("dictionary loader returned wrong table id: "
                        + loaded.id().value());
            }
            lock.lock();
            try {
                publishUnderLock(loaded);
                loading.remove(tableId, future);
            } finally {
                lock.unlock();
            }
        } catch (Throwable caught) {
            failure = caught;
            lock.lock();
            try {
                loading.remove(tableId, future);
            } finally {
                lock.unlock();
            }
        }
        // complete 可能同步执行 dependent action，必须放在 cache lock 外。
        if (failure == null) {
            future.complete(loaded);
        } else {
            future.completeExceptionally(failure);
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    private void awaitLoad(TableId tableId, CompletableFuture<TableDefinition> future, long remainingNanos) {
        if (remainingNanos <= 0) {
            throw new DictionaryCacheLoadException("dictionary cache load timeout: " + tableId.value());
        }
        try {
            future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new DictionaryCacheLoadException("dictionary cache load timeout: " + tableId.value(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DictionaryCacheLoadException("dictionary cache load interrupted: " + tableId.value(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error error) {
                throw error;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new DictionaryCacheLoadException("dictionary cache loader failed: " + tableId.value(), cause);
        }
    }

    private void publishUnderLock(TableDefinition table) {
        DictionaryVersion barrier = invalidatedAt.get(table.id());
        if (barrier != null) {
            if (table.version().compareTo(barrier) <= 0) {
                throw new DictionaryObjectNotFoundException("dictionary table version is blocked by DROP: table="
                        + table.id().value() + " version=" + table.version().value()
                        + " barrier=" + barrier.value());
            }
        }
        Entry previous = current.get(table.id());
        if (previous != null) {
            int comparison = table.version().compareTo(previous.value.version());
            if (comparison < 0) {
                throw new DictionaryVersionConflictException("cannot publish older dictionary table version");
            }
            if (comparison == 0) {
                if (!previous.value.equals(table)) {
                    throw new DictionaryVersionConflictException("same dictionary version has different table value");
                }
                previous.lastAccess = ++accessClock;
                invalidatedAt.remove(table.id());
                return;
            }
            previous.stale = true;
        }
        Entry entry = new Entry(table, ++accessClock);
        versions.computeIfAbsent(table.id(), ignored -> new TreeMap<>())
                .put(table.version().value(), entry);
        current.put(table.id(), entry);
        invalidatedAt.remove(table.id());
        if (previous != null && previous.pinCount == 0) {
            removeEntry(table.id(), previous);
        }
        evictToCapacity(entry);
    }

    private DictionaryPin<TableDefinition> pin(Entry entry) {
        entry.pinCount++;
        entry.lastAccess = ++accessClock;
        return new DictionaryPin<>(entry.value, entry.value.version(), () -> entry.stale,
                () -> release(entry));
    }

    private void release(Entry entry) {
        lock.lock();
        try {
            if (entry.pinCount <= 0) {
                throw new DictionaryCacheLoadException("dictionary pin count underflow");
            }
            entry.pinCount--;
            if (entry.pinCount == 0 && entry.stale) {
                removeEntry(entry.value.id(), entry);
            }
            pinsChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void evictToCapacity(Entry protectedEntry) {
        while (current.size() > capacity) {
            Entry victim = current.values().stream()
                    .filter(entry -> entry != protectedEntry && entry.pinCount == 0)
                    .min(Comparator.comparingLong(entry -> entry.lastAccess)).orElse(null);
            if (victim == null) {
                return;
            }
            current.remove(victim.value.id(), victim);
            victim.stale = true;
            removeEntry(victim.value.id(), victim);
        }
    }

    private void removeReleasedStale(TableId tableId, NavigableMap<Long, Entry> tableVersions) {
        List<Entry> released = new ArrayList<>();
        for (Entry entry : tableVersions.values()) {
            if (entry.stale && entry.pinCount == 0) {
                released.add(entry);
            }
        }
        for (Entry entry : released) {
            removeEntry(tableId, entry);
        }
    }

    private void removeEntry(TableId tableId, Entry entry) {
        NavigableMap<Long, Entry> tableVersions = versions.get(tableId);
        if (tableVersions == null) {
            return;
        }
        tableVersions.remove(entry.value.version().value(), entry);
        if (tableVersions.isEmpty()) {
            versions.remove(tableId);
        }
    }

    private boolean hasPins(TableId tableId) {
        NavigableMap<Long, Entry> tableVersions = versions.get(tableId);
        return tableVersions != null && tableVersions.values().stream().anyMatch(entry -> entry.pinCount > 0);
    }

    private static void validatePinArguments(TableId tableId, Duration timeout,
                                             DictionaryLoader<TableDefinition> loader) {
        if (tableId == null || timeout == null || timeout.isZero() || timeout.isNegative() || loader == null) {
            throw new DatabaseValidationException("pin table requires id, positive timeout and loader");
        }
    }

    private static long timeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("dictionary cache timeout is too large", overflow);
        }
    }

    private static long deadline(long timeoutNanos) {
        long now = System.nanoTime();
        long deadline = now + timeoutNanos;
        return deadline < 0 && now > 0 ? Long.MAX_VALUE : deadline;
    }

    private static long remaining(long deadline) {
        return deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
    }

    /** Entry 可变字段全部由 cache lock 保护；stale 额外 volatile 供 pin 的只读诊断。 */
    private static final class Entry {
        private final TableDefinition value;
        private int pinCount;
        private volatile boolean stale;
        private long lastAccess;

        private Entry(TableDefinition value, long lastAccess) {
            this.value = value;
            this.lastAccess = lastAccess;
        }
    }
}
