package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 低优先级 Change Buffer merge worker。每轮只从全局树选择有界数量的不同目标页，再通过普通 Buffer Pool
 * demand load 复用发布前拦截器；它不实现第二套页内 merge 算法，也不持有全局树 latch 执行目标页 IO。
 */
@Slf4j
public final class ChangeBufferMergeWorker implements AutoCloseable {

    /** worker 批量、唤醒周期与有界关闭配置；构造后不可变。 */
    private final ChangeBufferConfig config;
    /** 只读选择待 merge 目标页的 system.ibd 全局 mutation 树。 */
    private final ChangeBufferStore store;
    /** worker 扫描使用的只读 MTR 工厂；不跨目标页加载持有 MTR。 */
    private final MiniTransactionManager mtrManager;
    /** 触发普通 demand load 的唯一入口，实际 merge 由发布前拦截器完成。 */
    private final BufferPool pool;
    /**
     * 后台循环退出前发布 fatal 的组合根回调；生产实现关闭写闸门与用户流量，低层独立测试默认只保留 FAILED 状态。
     */
    private final Consumer<DatabaseFatalException> fatalFailureHandler;
    /** lifecycle/周期等待锁；不保护页或 Change Buffer 持久状态。 */
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    /** requestMerge/close 唤醒周期等待的条件变量，由 lifecycleLock 唯一保护。 */
    private final Condition wakeup = lifecycleLock.newCondition();
    /** 后台线程 finally 路径的一次性退出信号，供 close 做有界依赖交接。 */
    private final CountDownLatch stopped = new CountDownLatch(1);
    /** 线程安全的 worker 生命周期权威状态；状态转换不依赖隐式 monitor。 */
    private final AtomicReference<ChangeBufferWorkerState> state =
            new AtomicReference<>(ChangeBufferWorkerState.NEW);
    /** FAILED 状态对应的首个运行期根因；只由 worker 写、诊断线程读取。 */
    private volatile RuntimeException failure;

    /**
     * 创建尚未启动的后台 merge worker。
     *
     * @param config worker 批量、周期和关闭等待边界
     * @param store 用于选择 pending 目标页的全局 mutation store
     * @param mtrManager 只读选择 MTR 的同一 redo 域工厂
     * @param pool 通过首次加载触发发布前 merge 的 Buffer Pool
     */
    public ChangeBufferMergeWorker(ChangeBufferConfig config, ChangeBufferStore store,
                                   MiniTransactionManager mtrManager, BufferPool pool) {
        this(config, store, mtrManager, pool, ignored -> { });
    }

    /**
     * 创建带实例级 fail-stop 回调的后台 merge worker。
     *
     * @param config worker 批量、周期和关闭等待边界
     * @param store 用于选择 pending 目标页的全局 mutation store
     * @param mtrManager 只读选择 MTR 的同一 redo 域工厂
     * @param pool 通过首次加载触发发布前 merge 的 Buffer Pool
     * @param fatalFailureHandler worker 无法完成目标页加载或 merge 时的实例级 fail-stop 回调；不得为 {@code null}
     */
    public ChangeBufferMergeWorker(ChangeBufferConfig config, ChangeBufferStore store,
                                   MiniTransactionManager mtrManager, BufferPool pool,
                                   Consumer<DatabaseFatalException> fatalFailureHandler) {
        if (config == null || store == null || mtrManager == null || pool == null
                || fatalFailureHandler == null) {
            throw new DatabaseValidationException("change buffer worker dependencies must not be null");
        }
        this.config = config;
        this.store = store;
        this.mtrManager = mtrManager;
        this.pool = pool;
        this.fatalFailureHandler = fatalFailureHandler;
    }

    /**
     * 启动唯一 daemon worker；重复启动被拒绝，避免同一实例建立两套后台所有权。
     *
     * @throws ChangeBufferStateException 当前状态不是 NEW 时抛出，既有线程状态不改变
     */
    public void start() {
        if (!state.compareAndSet(ChangeBufferWorkerState.NEW, ChangeBufferWorkerState.RUNNING)) {
            throw new ChangeBufferStateException("change buffer worker cannot start from " + state.get());
        }
        Thread.ofPlatform().daemon(true).name("change-buffer-merge").start(this::runLoop);
    }

    /** @return 当前显式生命周期状态。 */
    public ChangeBufferWorkerState state() {
        return state.get();
    }

    /** @return FAILED 时的首个根因；其它状态为空。 */
    public Optional<RuntimeException> failure() {
        return Optional.ofNullable(failure);
    }

    /**
     * 唤醒 worker 提前执行下一轮；不创建新线程也不等待完成。NEW/FAILED/STOPPED 调用只发无害 signal。
     */
    public void requestMerge() {
        lifecycleLock.lock();
        try {
            wakeup.signalAll();
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * 执行单线程后台循环；每轮只选择有界目标并通过 Buffer Pool 触发统一 merge 路径。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在只读 MTR 内取得有界目标页列表，提交后才开始用户页 IO，避免跨空间持有系统页 latch。</li>
     *     <li>跳过已经驻留且按发布不变量完成 merge 的页，冷页则以普通共享 getPage 触发拦截器。</li>
     *     <li>一轮完成后在显式 Condition 上有界等待；requestMerge 或 close 可以提前唤醒。</li>
     *     <li>正常停止发布 STOPPED；异常发布 FAILED 与根因，并始终释放 close 等待者。</li>
     * </ol>
     */
    private void runLoop() {
        try {
            while (state.get() == ChangeBufferWorkerState.RUNNING) {
                // 1、selectTargets 在只读 MTR 提交后返回，循环体不持 system.ibd latch。
                for (PageId target : selectTargets()) {
                    if (state.get() != ChangeBufferWorkerState.RUNNING) {
                        break;
                    }
                    // 2、已驻留页按不变量应已在首次发布前 merge；冷页统一走 getPage 发布协议。
                    if (pool.isResident(target)) {
                        continue;
                    }
                    try (PageGuard ignored = pool.getPage(target, PageLatchMode.SHARED)) {
                        // getPage 只有在 interceptor 完成并提交后才返回；关闭 guard 仅释放本次预取 fix/S latch。
                    }
                }
                // 3、周期等待可被显式 merge 请求或关闭请求唤醒，且不持任何页资源。
                awaitNextTick();
            }
            // 4、只有正常退出循环才把 STOPPING 收敛为 STOPPED；异常路径保留 FAILED。
            state.compareAndSet(ChangeBufferWorkerState.STOPPING, ChangeBufferWorkerState.STOPPED);
        } catch (RuntimeException backgroundFailure) {
            failure = backgroundFailure;
            state.set(ChangeBufferWorkerState.FAILED);
            ChangeBufferWorkerFailureException fatal = new ChangeBufferWorkerFailureException(
                    "change buffer merge worker cannot safely continue", backgroundFailure);
            try {
                fatalFailureHandler.accept(fatal);
            } catch (RuntimeException publicationFailure) {
                fatal.addSuppressed(publicationFailure);
                log.error("change buffer merge worker failed to publish instance fail-stop state", fatal);
            }
            log.error("change buffer merge worker failed; buffered evidence remains durable", backgroundFailure);
        } finally {
            stopped.countDown();
        }
    }

    /**
     * 在独立只读 MTR 中物化本轮有界目标页列表，返回前释放全部 system.ibd latch/fix。
     *
     * @return 按全局 key 首次出现顺序去重的不可变 PageId 列表，大小不超过配置 batch
     * @throws ChangeBufferFormatException 全局树不可遍历时抛出，由 worker 外层提升为实例 fatal
     * @throws ChangeBufferStateException header/tree 状态无法有界选择时抛出，由 worker 外层提升为实例 fatal
     */
    private List<PageId> selectTargets() {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            List<PageId> result = store.firstTargetPages(read, config.mergeBatchPages());
            mtrManager.commit(read);
            return result;
        } catch (RuntimeException error) {
            if (read.state() == MiniTransactionState.ACTIVE) {
                try {
                    mtrManager.rollbackUncommitted(read);
                } catch (RuntimeException releaseFailure) {
                    error.addSuppressed(releaseFailure);
                }
            }
            throw error;
        }
    }

    /**
     * 在不持页、MTR 或 gate 资源时等待下一 tick；request/close signal 或线程中断均可提前结束等待。
     * 中断会恢复线程中断位并把 RUNNING 收敛为 STOPPING。
     */
    private void awaitNextTick() {
        lifecycleLock.lock();
        try {
            if (state.get() == ChangeBufferWorkerState.RUNNING) {
                try {
                    wakeup.awaitNanos(saturatedNanos(config.mergeInterval()));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    state.compareAndSet(ChangeBufferWorkerState.RUNNING, ChangeBufferWorkerState.STOPPING);
                }
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * 有界停止 worker。超时表示依赖仍可能被后台线程访问，调用方不得继续关闭 Buffer Pool/PageStore。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>NEW 状态原子转 STOPPED并完成 latch，证明线程从未启动。</li>
     *     <li>RUNNING 状态转 STOPPING并 signal Condition，后台循环会在当前 target/record 边界退出。</li>
     *     <li>以配置 stopTimeout 等待 stopped latch；FAILED/STOPPED 同样走一次有界依赖交接。</li>
     *     <li>超时或中断抛领域异常且不关闭依赖；成功返回后组合根才可关闭 pool/store/redo。</li>
     * </ol>
     *
     * @throws ChangeBufferStateException worker 未在上限内停止或关闭线程被中断时抛出
     */
    @Override
    public void close() {
        // 1、未启动 worker 不创建线程，直接发布已停止终态。
        ChangeBufferWorkerState current = state.get();
        if (current == ChangeBufferWorkerState.NEW) {
            if (state.compareAndSet(ChangeBufferWorkerState.NEW, ChangeBufferWorkerState.STOPPED)) {
                stopped.countDown();
                return;
            }
            current = state.get();
        }
        // 2、运行中只请求协作停止，不使用 interrupt 打断正在进行的 Buffer Pool/MTR 操作。
        if (current == ChangeBufferWorkerState.RUNNING) {
            state.compareAndSet(ChangeBufferWorkerState.RUNNING, ChangeBufferWorkerState.STOPPING);
            requestMerge();
        }
        // 3、有界等待后台 finally 的唯一 stopped 信号。
        try {
            if (!stopped.await(saturatedNanos(config.stopTimeout()), TimeUnit.NANOSECONDS)) {
                // 4、依赖仍可能被 worker 使用，调用方收到失败后必须停止后续资源关闭。
                throw new ChangeBufferStateException("timed out stopping change buffer merge worker");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            // 4、中断不伪造成功；依赖所有权仍保留给当前引擎实例。
            throw new ChangeBufferStateException("interrupted stopping change buffer merge worker", interrupted);
        }
        // 4、stopped 已证明后台不再访问依赖，组合根现在可以继续关闭 pool/store/redo。
    }

    /**
     * 把已校验为正的等待时间转换为纳秒；极大配置饱和到 {@link Long#MAX_VALUE}，不能因换算溢出使
     * 后台线程或关闭流程异常退出。
     *
     * @param duration 配置提供的正等待时间
     * @return 可传给显式 Condition/Latch 的正纳秒上限
     */
    private static long saturatedNanos(java.time.Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
