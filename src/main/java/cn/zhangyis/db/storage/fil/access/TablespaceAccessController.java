package cn.zhangyis.db.storage.fil.access;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 按表空间分片的操作级准入控制器。
 *
 * <p>普通 page/FSP/flush 路径获取共享 lease；truncate/discard 获取独占 lease。公平读写锁避免持续普通 IO
 * 饿死维护操作。锁只按 {@link SpaceId} 分片，不使用全局大锁；等待有统一超时且支持线程中断。
 * 独占 owner 可在同线程内重入共享锁，供截断服务调用维护 MTR/flush，但不得把 lease 跨线程转交。</p>
 *
 * <p>控制器只解决进程内“普通操作与生命周期操作不能交叉”的互斥问题。它不读取 page0，
 * 也不判断表空间是否 {@code CORRUPTED}/{@code INACTIVE}；调用方必须在成功获取 lease 后
 * 通过 registry 重新校验状态，消除等待期间状态变化造成的先检后用竞态。</p>
 */
public final class TablespaceAccessController {

    /**
     * 生产默认等待上界；所有未显式配置超时的准入请求共享该值，防止维护流程因物理访问
     * 泄漏而无界阻塞。
     */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * 每个 {@link SpaceId} 对应一把公平读写锁。并发映射负责安全发布锁实例，键不会在控制器
     * 生命周期内移除，以免同一表空间的新旧锁实例同时生效而破坏互斥。
     */
    private final ConcurrentMap<SpaceId, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    /**
     * 单次 lease 获取的纳秒等待上界；构造时完成单位换算，后续获取路径不再承担溢出风险。
     */
    private final long timeoutNanos;

    /**
     * 使用生产默认超时创建控制器。
     */
    public TablespaceAccessController() {
        this(DEFAULT_TIMEOUT);
    }

    /**
     * 使用指定等待上界创建控制器。
     *
     * @param timeout 每次共享或独占 lease 的最大等待时间；必须非空且严格大于零，并且能够精确换算为
     *                {@code long} 纳秒
     * @throws DatabaseValidationException 超时为空、非正数或换算纳秒溢出时抛出；对象不会被创建
     */
    public TablespaceAccessController(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("tablespace access timeout must be positive");
        }
        try {
            this.timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("tablespace access timeout is too large", overflow);
        }
    }

    /**
     * 获取普通访问共享 lease。
     *
     * <p>MTR 应在第一次 fix 该空间页前获取，并在所有 page guard 之后释放，从而保证持有页面
     * 引用期间 truncate/drop 无法获得独占准入权。返回成功只表示当前进程内准入成立，调用方仍须
     * 在 lease 内复核 registry 状态。</p>
     *
     * @param spaceId 目标表空间的稳定标识；不能为空
     * @return 由当前线程持有且可幂等关闭的共享 lease；不会返回 {@code null}
     * @throws DatabaseValidationException spaceId 为空时抛出；调用方应修正调用参数
     * @throws TablespaceAccessTimeoutException 等待超时或线程被中断时抛出；未获得任何准入权，
     *                                         调用方可回滚当前操作或稍后重试
     */
    public TablespaceAccessLease acquireShared(SpaceId spaceId) {
        return acquire(spaceId, false);
    }

    /**
     * 获取生命周期维护独占 lease。
     *
     * <p>返回前同一表空间的其它共享 owner 已全部退出，且新的共享获取会被阻塞；调用方可在
     * lease 内执行状态持久化、缓存失效、截断或关闭。其它 {@link SpaceId} 不受影响。</p>
     *
     * @param spaceId 待执行生命周期变更的表空间标识；不能为空
     * @return 由当前线程持有且可幂等关闭的独占 lease；不会返回 {@code null}
     * @throws DatabaseValidationException spaceId 为空时抛出；调用方应修正调用参数
     * @throws TablespaceAccessTimeoutException 等待旧 owner 退出超时或线程被中断时抛出；独占操作
     *                                         尚未开始，调用方不得继续执行 truncate/drop
     */
    public TablespaceAccessLease acquireExclusive(SpaceId spaceId) {
        return acquire(spaceId, true);
    }

    /**
     * 为后台维护立即尝试取得公平独占 lease。
     *
     * <p>该入口使用零时长 timed {@code tryLock}，既不进入无界等待，也遵守公平锁上已经排队的普通 owner；
     * 返回 empty 表示本轮维护应释放其它短资源并在未来 cycle 重试，不能把竞争解释为存储错误。</p>
     *
     * @param spaceId 待维护表空间的稳定标识；不得为 {@code null}
     * @return 当前线程取得的独占 lease，竞争存在时为空；从不返回 Java {@code null}
     * @throws DatabaseValidationException spaceId 为空时抛出，调用方应修正请求
     * @throws TablespaceAccessTimeoutException 零等待过程中线程被中断时抛出；中断标志被恢复且未取得 lease
     */
    public Optional<TablespaceAccessLease> tryAcquireExclusive(SpaceId spaceId) {
        // 1、在创建分片锁前拒绝无身份请求，避免锁表残留无领域意义的键。
        requireSpaceId(spaceId);
        // 2、复用同一公平锁；timed tryLock(0) 不越过已排队 owner，也不等待当前共享 owner。
        ReentrantReadWriteLock rw = locks.computeIfAbsent(spaceId, ignored -> new ReentrantReadWriteLock(true));
        Lock lock = rw.writeLock();
        try {
            if (!lock.tryLock(0L, TimeUnit.NANOSECONDS)) {
                return Optional.empty();
            }
        } catch (InterruptedException interrupted) {
            // 3、中断没有获得 ownership；保留标志并把取消边界交给后台 driver。
            Thread.currentThread().interrupt();
            throw new TablespaceAccessTimeoutException(
                    "interrupted trying exclusive tablespace lease: " + spaceId.value(), interrupted);
        }
        // 4、成功路径与阻塞 API 共用 owner-bound lease，确保异常和重复 close 只解锁一次。
        return Optional.of(lease(lock));
    }

    /**
     * 在目标表空间的公平读写锁上获取指定模式，并把解锁动作封装为线程绑定 lease。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先拒绝空表空间标识，避免为无效键创建无法回收且没有领域含义的锁实例。</li>
     *     <li>取得该 {@link SpaceId} 唯一的公平读写锁并选择共享或独占模式；该阶段只修改锁映射，
     *     不执行文件 IO，也不接触 page/registry 状态。</li>
     *     <li>在统一上界内响应中断地等待锁；失败时不生成 lease，中断状态会恢复给上层。</li>
     *     <li>捕获 owner 与一次性关闭状态，返回只能由获取线程释放的幂等 lease，确保重复
     *     {@code close()} 不会多次解锁。</li>
     * </ol>
     *
     * @param spaceId 目标表空间标识；不能为空
     * @param exclusive {@code true} 获取生命周期独占权，{@code false} 获取普通操作共享权
     * @return 当前线程拥有的准入 lease；关闭后对应锁模式被释放
     * @throws DatabaseValidationException spaceId 为空，或 lease 被非 owner 线程关闭时抛出
     * @throws TablespaceAccessTimeoutException 获取等待超时或被中断时抛出；不会遗留锁 ownership
     */
    private TablespaceAccessLease acquire(SpaceId spaceId, boolean exclusive) {
        // 1. 无效标识没有可保护的物理资源，必须在创建分片锁之前失败。
        requireSpaceId(spaceId);

        // 2. 同一 SpaceId 始终复用公平锁；不同表空间的普通 IO 与维护操作可并行。
        ReentrantReadWriteLock rw = locks.computeIfAbsent(spaceId, ignored -> new ReentrantReadWriteLock(true));
        Lock lock = exclusive ? rw.writeLock() : rw.readLock();

        // 3. 有界、可中断地等待准入；失败路径既不发布 lease，也不持有目标锁。
        try {
            if (!lock.tryLock(timeoutNanos, TimeUnit.NANOSECONDS)) {
                throw new TablespaceAccessTimeoutException("timed out acquiring "
                        + (exclusive ? "exclusive" : "shared") + " tablespace lease: " + spaceId.value());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TablespaceAccessTimeoutException("interrupted acquiring tablespace lease: "
                    + spaceId.value(), interrupted);
        }

        // 4. 将 owner 和一次性释放门封装进 lease，保证异常清理及重复 close 的解锁语义稳定。
        return lease(lock);
    }

    /** 把已经取得的锁封装为线程绑定、幂等关闭 lease；调用方必须先成功获得对应锁。 */
    private static TablespaceAccessLease lease(Lock lock) {
        Thread owner = Thread.currentThread();
        AtomicBoolean closed = new AtomicBoolean();
        return () -> {
            if (Thread.currentThread() != owner) {
                throw new DatabaseValidationException("tablespace lease must be closed by owner thread");
            }
            if (closed.compareAndSet(false, true)) {
                lock.unlock();
            }
        };
    }

    /** 在访问分片锁映射前验证物理身份，失败不留下锁实例。 */
    private static void requireSpaceId(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("tablespace access space id must not be null");
        }
    }
}
