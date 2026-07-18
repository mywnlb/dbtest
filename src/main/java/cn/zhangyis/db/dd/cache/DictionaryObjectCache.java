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

    /**
     * 创建 {@code DictionaryObjectCache}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DictionaryObjectCache(int capacity) {
        if (capacity <= 0) {
            throw new DatabaseValidationException("dictionary cache capacity must be positive");
        }
        this.capacity = capacity;
    }

    /**
     * pin 最新 table 版本。leader 在锁外 load，followers 对同一 future 做有界等待；load 成功后所有线程回到
     * lock 内重新 pin current，避免 publish/invalidate 竞态下返回已不再 current 的裸对象。
     *
     * @param tableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param loader 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code pinTable} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws DictionaryObjectNotFoundException 按稳定身份无法定位所需领域对象时抛出；调用方应刷新元数据或终止当前操作
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

    /** 发布新 current version；旧 current 只被标 stale，pin owner 仍可安全读完。
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     *
     * @param tableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param dropVersion 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryVersionConflictException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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

    /** DROP 在持有 MDL X 后有界等待所有历史 pin 释放；timeout 返回 false，不吞掉资源泄漏。
     *
     * @param tableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @return 在超时或取消前观察到 {@code awaitUnpinned} 的目标状态时为 {@code true}；等待期限届满且状态仍未满足时为 {@code false}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /**
     * 采集 {@code snapshot} 对应的数据字典稳定快照；返回对象与后续内部修改隔离，不转移内部可变状态的所有权。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
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

    /**
     * 推进 {@code completeLoad} 对应的数据字典阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param tableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param loader 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param future 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DictionaryCacheLoadException 字典对象装载失败、版本不匹配或并发装载异常完成时抛出；调用方应释放 pin，保留 cause，并按新版本重试
     */
    private void completeLoad(TableId tableId, DictionaryLoader<TableDefinition> loader,
                              CompletableFuture<TableDefinition> future) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        TableDefinition loaded = null;
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        Throwable failure = null;
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        if (failure == null) {
            future.complete(loaded);
        } else {
            future.completeExceptionally(failure);
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    /**
     * 按数据字典并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param tableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param future 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param remainingNanos 参与 {@code awaitLoad} 的时间量 {@code remainingNanos}；必须非负，零表示立即检查或尚未累计等待
     * @throws DictionaryCacheLoadException 字典对象装载失败、版本不匹配或并发装载异常完成时抛出；调用方应释放 pin，保留 cause，并按新版本重试
     */
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

    /**
     * 按数据字典并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DictionaryObjectNotFoundException 按稳定身份无法定位所需领域对象时抛出；调用方应刷新元数据或终止当前操作
     * @throws DictionaryVersionConflictException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private void publishUnderLock(TableDefinition table) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        DictionaryVersion barrier = invalidatedAt.get(table.id());
        if (barrier != null) {
            if (table.version().compareTo(barrier) <= 0) {
                throw new DictionaryObjectNotFoundException("dictionary table version is blocked by DROP: table="
                        + table.id().value() + " version=" + table.version().value()
                        + " barrier=" + barrier.value());
            }
        }
        Entry previous = current.get(table.id());
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        current.put(table.id(), entry);
        invalidatedAt.remove(table.id());
        if (previous != null && previous.pinCount == 0) {
            removeEntry(table.id(), previous);
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        evictToCapacity(entry);
    }

    private DictionaryPin<TableDefinition> pin(Entry entry) {
        entry.pinCount++;
        entry.lastAccess = ++accessClock;
        return new DictionaryPin<>(entry.value, entry.value.version(), () -> entry.stale,
                () -> release(entry));
    }

    /**
     * 释放本方法拥有的数据字典资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param entry 当前 cache 或 pin 所持有的已装载条目；不得为 {@code null}，引用计数和所有权必须归属当前 cache key
     * @throws DictionaryCacheLoadException 字典对象装载失败、版本不匹配或并发装载异常完成时抛出；调用方应释放 pin，保留 cause，并按新版本重试
     */
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

    /**
     * 更新 {@code removeReleasedStale} 指定的数据字典局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param tableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param tableVersions 参与 {@code removeReleasedStale} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     */
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
        /**
         * 构造时冻结的 {@code value} 领域快照；其身份、版本与范围来自同一次权威读取，下游步骤依赖它检测并发变化和避免发布陈旧状态。
         */
        private final TableDefinition value;
        /**
         * 记录 {@code pinCount} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
         */
        private int pinCount;
        /**
         * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
         */
        private volatile boolean stale;
        /**
         * 记录 {@code lastAccess} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private long lastAccess;

        private Entry(TableDefinition value, long lastAccess) {
            this.value = value;
            this.lastAccess = lastAccess;
        }
    }
}
