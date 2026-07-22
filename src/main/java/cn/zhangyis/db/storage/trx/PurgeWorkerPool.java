package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 有界 purge worker pool 与 table-token 调度器。
 *
 * <p>每个 table id 的 tail 指向该表上一条日志的 completion stage；后继只有在全部前驱 READY 后才被提交给
 * executor。依赖等待本身不占 worker 线程。一个 history log 涉及多表时同时更新全部 table tail，从而保持同表
 * 物理 history 顺序。空表集合使用内部串行 token，兼容低层旧 history 条目。</p>
 */
final class PurgeWorkerPool implements AutoCloseable {

    /** 生产 history table id 必须为正，零可安全作为 legacy 空集合的内部串行 token。 */
    private static final long LEGACY_SERIAL_TABLE_TOKEN = 0L;
    /** daemon worker 名称序号，仅用于诊断，不表达任务顺序。 */
    private static final AtomicInteger THREAD_SEQUENCE = new AtomicInteger();

    /** 不可变资源上限。 */
    private final PurgeConfig config;
    /** direct 模式使用调用线程；parallel 模式使用有界 ThreadPoolExecutor。 */
    private final Executor executor;
    /** parallel 模式的生命周期 owner；direct 模式为空。 */
    private final ThreadPoolExecutor executorService;
    /** 防止两个 dispatcher 同时覆盖 active futures；上层 batch gate 之外再做内部防御。 */
    private final ReentrantLock batchLock = new ReentrantLock();
    /** 保护 state 与 active futures，绝不跨 worker 等待持有。 */
    private final ReentrantLock stateLock = new ReentrantLock();
    /** 当前生命周期状态。 */
    private PurgeWorkerPoolState state = PurgeWorkerPoolState.RUNNING;
    /** 当前批全部 completion stage；stop 用于完成取消并唤醒 dispatcher。 */
    private List<CompletableFuture<PurgeLogTaskResult>> activeFutures = List.of();

    /**
     * 创建 direct 或平台线程模式的调度器；平台线程惰性启动，构造期不访问 history 或页面。
     *
     * @param config 已通过领域校验的线程数、窗口与超时
     * @param direct true 表示在 dispatcher 线程执行；false 表示创建有界线程池 owner
     */
    private PurgeWorkerPool(PurgeConfig config, boolean direct) {
        this.config = config;
        if (direct) {
            this.executorService = null;
            this.executor = Runnable::run;
            return;
        }
        this.executorService = new ThreadPoolExecutor(config.workerCount(), config.workerCount(),
                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(config.maxInFlightLogs()), runnable -> {
                    Thread thread = new Thread(runnable,
                            "minimysql-purge-worker-" + THREAD_SEQUENCE.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        this.executor = executorService;
    }

    /**
     * 创建不持有平台线程、仍执行相同 table-token 语义的兼容 executor。
     *
     * @param config workerCount 必须为 1 的兼容边界
     * @return 生命周期处于 RUNNING 的 direct pool
     * @throws DatabaseValidationException 配置为空或 workerCount 不是 1 时抛出
     */
    static PurgeWorkerPool direct(PurgeConfig config) {
        if (config == null || config.workerCount() != 1) {
            throw new DatabaseValidationException("direct purge pool requires one-worker config");
        }
        return new PurgeWorkerPool(config, true);
    }

    /**
     * 创建生产并行 pool；线程在首个独立 table lane 提交时惰性创建。
     *
     * @param config 已校验的生产并发边界
     * @return 生命周期处于 RUNNING、队列受 maxInFlightLogs 约束的 pool
     * @throws DatabaseValidationException 配置为空时抛出
     */
    static PurgeWorkerPool parallel(PurgeConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("parallel purge config must not be null");
        }
        return new PurgeWorkerPool(config, false);
    }

    /**
     * 执行一个有界物理 history 前缀，并返回与输入相同顺序的稳定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验批次、pool 状态和单批容量；失败时尚未提交 worker。</li>
     *     <li>按输入物理顺序建立 table tail DAG；依赖 stage 只在前驱完成后提交 action，不占 worker 等待。</li>
     *     <li>发布 active futures 后有界等待全批；timeout/中断使 pool fail-stop并取消调度，但不打断记录内页修改。</li>
     *     <li>按输入顺序提取结果并清除 active 引用；每条领域失败仍作为 FAILED 值交给 dispatcher统一收口。</li>
     * </ol>
     *
     * @param works head 开始、物理顺序稳定且数量不超过配置窗口的日志任务
     * @return 与 works 一一对应、顺序相同的不可变结果
     * @throws DatabaseValidationException 输入为空引用、超过窗口或并发调用本 pool 时抛出
     * @throws PurgeBatchTimeoutException worker 未在 batchTimeout 内全部到达稳定边界时抛出
     * @throws PurgeWorkerStoppedException 组合根已请求停止时抛出
     * @throws PurgeWorkerExecutionException completion 基础设施异常时抛出并保留 cause
     */
    List<PurgeLogTaskResult> execute(List<PurgeLogWork> works) {
        // 1、先校验不可变输入和唯一批次 owner，避免部分提交后才发现容量错误。
        if (works == null || works.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("purge works must not contain null");
        }
        if (works.size() > config.maxInFlightLogs()) {
            throw new DatabaseValidationException("purge work count exceeds maxInFlightLogs: count="
                    + works.size() + ", max=" + config.maxInFlightLogs());
        }
        requireRunning();
        if (!batchLock.tryLock()) {
            throw new DatabaseValidationException("purge worker pool already owns an active batch");
        }
        try {
            if (works.isEmpty()) {
                return List.of();
            }

            // 2、每个 table tail 只保存 completion stage；worker 不在内部等待 predecessor。
            Map<Long, CompletableFuture<PurgeLogTaskResult>> tableTails = new HashMap<>();
            List<CompletableFuture<PurgeLogTaskResult>> futures = new ArrayList<>(works.size());
            for (PurgeLogWork work : works) {
                Set<Long> tableTokens = tableTokens(work.entry());
                List<CompletableFuture<PurgeLogTaskResult>> predecessors = tableTokens.stream()
                        .map(tableTails::get)
                        .filter(java.util.Objects::nonNull)
                        .distinct()
                        .toList();
                CompletableFuture<Void> dependency = CompletableFuture.allOf(
                        predecessors.toArray(CompletableFuture[]::new));
                CompletableFuture<PurgeLogTaskResult> future = dependency.thenApplyAsync(ignored -> {
                    if (predecessors.stream().anyMatch(previous -> !previous.join().releasesTableLane())) {
                        return PurgeLogTaskResult.blocked(work.entry());
                    }
                    return invoke(work);
                }, executor);
                futures.add(future);
                for (long token : tableTokens) {
                    tableTails.put(token, future);
                }
            }
            publishActiveFutures(futures);

            // 3、dispatcher 等待有明确 timeout；失败后取消 stage 并关闭接纳，运行任务只在自身记录边界停止。
            CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
            try {
                all.get(waitNanos(config.batchTimeout()), TimeUnit.NANOSECONDS);
            } catch (TimeoutException timeout) {
                failPool(futures);
                throw new PurgeBatchTimeoutException(
                        "purge worker batch timed out after " + config.batchTimeout(), timeout);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                boolean stopping = isStopping();
                failPool(futures);
                if (stopping) {
                    throw new PurgeWorkerStoppedException("purge worker batch interrupted by shutdown", interrupted);
                }
                throw new PurgeWorkerExecutionException("purge worker batch interrupted", interrupted);
            } catch (ExecutionException execution) {
                if (isStopping()) {
                    throw new PurgeWorkerStoppedException(
                            "purge worker batch cancelled by shutdown", execution.getCause());
                }
                failPool(futures);
                throw new PurgeWorkerExecutionException(
                        "purge worker completion failed", execution.getCause());
            }

            // 4、all 已成功完成，join 不再等待；返回值保持原物理顺序供 dispatcher finalization。
            return futures.stream().map(CompletableFuture::join).toList();
        } finally {
            clearActiveFutures();
            batchLock.unlock();
        }
    }

    /**
     * 请求停止并取消尚未开始的 completion stage。运行中的 action 不接受线程中断，因为中断可能落在 B+Tree/MTR
     * 物理修改中间；上层 coordinator 在每条 undo record 开始前读取 pool 状态并安全退出。
     */
    void requestStop() {
        List<CompletableFuture<PurgeLogTaskResult>> futures;
        stateLock.lock();
        try {
            if (state == PurgeWorkerPoolState.STOPPED || state == PurgeWorkerPoolState.FAILED) {
                return;
            }
            state = PurgeWorkerPoolState.STOPPING;
            futures = activeFutures;
        } finally {
            stateLock.unlock();
        }
        futures.forEach(future -> future.cancel(true));
        if (executorService == null) {
            markStopped();
        } else {
            executorService.shutdown();
        }
    }

    /**
     * 在调用方共享关闭预算内等待平台线程退出。
     *
     * @param timeout 当前绝对 deadline 的剩余非负预算
     * @return direct/已终止 pool 返回 true；预算耗尽仍有线程时返回 false
     */
    boolean awaitStopped(Duration timeout) {
        long nanos = waitNanos(timeout);
        if (executorService == null) {
            return state() == PurgeWorkerPoolState.STOPPED || state() == PurgeWorkerPoolState.FAILED;
        }
        try {
            boolean terminated = executorService.awaitTermination(nanos, TimeUnit.NANOSECONDS);
            if (terminated && state() == PurgeWorkerPoolState.STOPPING) {
                markStopped();
            }
            return terminated;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 在短状态锁内读取生命周期，不等待 dispatcher 或 worker。
     *
     * @return 当前 pool 生命周期快照
     */
    PurgeWorkerPoolState state() {
        stateLock.lock();
        try {
            return state;
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 发布停止并使用批次 timeout 等待线程；生产组合根使用显式 request/await 共享更严格的全局 deadline。
     */
    @Override
    public void close() {
        requestStop();
        awaitStopped(config.batchTimeout());
    }

    /**
     * 在 worker 线程执行单条 history log 动作，把领域异常转换为有序 FAILED 结果；调度基础设施异常不在此吞并。
     *
     * @param work 已通过 execute 入口校验且 table dependency 已满足的动作
     * @return 与 work entry 身份一致的稳定状态；领域失败保留 cause
     */
    private PurgeLogTaskResult invoke(PurgeLogWork work) {
        try {
            PurgeLogTaskResult result = work.action().call();
            if (result == null || !result.entry().equals(work.entry())) {
                throw new DatabaseValidationException("purge worker returned mismatched/null result");
            }
            return result;
        } catch (DatabaseRuntimeException failure) {
            return PurgeLogTaskResult.failed(work.entry(), 0, 0, 0, failure);
        } catch (RuntimeException failure) {
            return PurgeLogTaskResult.failed(work.entry(), 0, 0, 0,
                    new PurgeWorkerExecutionException("purge log action failed", failure));
        } catch (Exception failure) {
            return PurgeLogTaskResult.failed(work.entry(), 0, 0, 0,
                    new PurgeWorkerExecutionException("purge log action reported checked failure", failure));
        }
    }

    /**
     * 提取日志涉及的全部 table token；旧条目没有 affected-table 证据时退回全局兼容 token，禁止错误并行。
     *
     * @param entry committed history 的不可变运行时投影
     * @return 非空、去重的调度 token 集合
     */
    private static Set<Long> tableTokens(HistoryEntry entry) {
        if (entry.affectedTableIds().isEmpty()) {
            return Set.of(LEGACY_SERIAL_TABLE_TOKEN);
        }
        return new LinkedHashSet<>(entry.affectedTableIds());
    }

    /** @throws PurgeWorkerStoppedException pool 已停止或 fail-stop 时拒绝新批次 */
    private void requireRunning() {
        PurgeWorkerPoolState current = state();
        if (current != PurgeWorkerPoolState.RUNNING) {
            throw new PurgeWorkerStoppedException("purge worker pool is not running: " + current);
        }
    }

    /**
     * 原子发布当前批 stage，供关闭线程取消；若停止已先发生，则取消全部 stage 并拒绝半发布批次。
     *
     * @param futures 与输入日志顺序一致的本批 completion stage
     * @throws PurgeWorkerStoppedException 发布时 pool 已不再接纳工作
     */
    private void publishActiveFutures(List<CompletableFuture<PurgeLogTaskResult>> futures) {
        stateLock.lock();
        try {
            if (state != PurgeWorkerPoolState.RUNNING) {
                futures.forEach(future -> future.cancel(true));
                throw new PurgeWorkerStoppedException("purge worker pool stopped while scheduling batch");
            }
            activeFutures = List.copyOf(futures);
        } finally {
            stateLock.unlock();
        }
    }

    /** 清除本批 stage 强引用；仅在 dispatcher 已返回或抛错后调用。 */
    private void clearActiveFutures() {
        stateLock.lock();
        try {
            activeFutures = List.of();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 把 pool 固定为 FAILED，取消排队 stage 并关闭 executor 接纳；运行 action 不被中断，必须自行到记录边界退出。
     *
     * @param futures 当前批全部 stage，用于唤醒 dispatcher 并阻止依赖后继执行
     */
    private void failPool(List<CompletableFuture<PurgeLogTaskResult>> futures) {
        stateLock.lock();
        try {
            state = PurgeWorkerPoolState.FAILED;
        } finally {
            stateLock.unlock();
        }
        futures.forEach(future -> future.cancel(true));
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /** @return 关闭请求已发布但平台线程可能尚未退出时为 true */
    private boolean isStopping() {
        PurgeWorkerPoolState current = state();
        return current == PurgeWorkerPoolState.STOPPING || current == PurgeWorkerPoolState.STOPPED;
    }

    /** 仅把正常 STOPPING 转为 STOPPED；FAILED 诊断状态不会被关闭覆盖。 */
    private void markStopped() {
        stateLock.lock();
        try {
            if (state == PurgeWorkerPoolState.STOPPING) {
                state = PurgeWorkerPoolState.STOPPED;
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 把非负 Duration 转为 awaitTermination 可消费的纳秒数，溢出时饱和。
     *
     * @param timeout 共享 deadline 的剩余预算；允许为零
     * @return 非负纳秒预算
     * @throws DatabaseValidationException timeout 为空或为负时抛出
     */
    private static long waitNanos(Duration timeout) {
        if (timeout == null || timeout.isNegative()) {
            throw new DatabaseValidationException("purge worker wait timeout must be non-null and non-negative");
        }
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
