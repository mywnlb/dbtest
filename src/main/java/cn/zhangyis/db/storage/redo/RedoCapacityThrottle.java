package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.concurrent.locks.LockSupport;

/**
 * redo capacity 前台反压协作者。
 *
 * <p>它只在 redo append/reservation 入口之前读取 current/checkpoint LSN，按 {@link RedoCapacityPolicy} 判断压力，
 * 并通过注入的 flush/checkpoint 动作推动后台边界前进。该类不直接访问 Buffer Pool frame、page latch、redo 文件仓储
 * 或 data file，从而保持 redo 层不反向依赖 flush/buffer 实现；调用方必须保证调用点早于 page latch、frame lock、
 * FSP/fil lease 等可能阻塞的资源获取。
 */
public final class RedoCapacityThrottle {

    /** 不做任何反压的实例，保留现有内存 redo 和单元测试构造器语义。 */
    public static final RedoCapacityThrottle NO_OP = new RedoCapacityThrottle();

    /** checkpoint age 到压力等级的策略。 */
    private final RedoCapacityPolicy capacityPolicy;
    /** 当前 redo 分配边界来源；通常为 {@link RedoLogManager#currentLsn()}。 */
    private final Supplier<Lsn> currentLsnSupplier;
    /** 最近 checkpoint/reclaim boundary 来源；由 flush/checkpoint 模块提供。 */
    private final Supplier<Lsn> checkpointLsnSupplier;
    /** ASYNC_FLUSH 下的非阻塞请求动作；通常只唤醒 page cleaner。 */
    private final Runnable asyncFlushRequest;
    /** SYNC_FLUSH/HARD_LIMIT 下推进 redo flush、dirty page flush 与 checkpoint 的一次动作。 */
    private final Runnable blockingFlushOnce;
    /** SYNC_FLUSH/HARD_LIMIT 前台最多等待时间。 */
    private final Duration timeout;
    /** no-op 标记，避免默认构造参与任何 LSN 读取或等待。 */
    private final boolean noOp;
    /** repository 允许一个 sealed batch 占用的最大物理字节；ring 模式为单文件大小。 */
    private final long maxPhysicalBatchBytes;
    /**
     * 已授权但尚未释放的前台 redo 预算。它不是 LSN 权威，只是 log-free-check 的保守账本：
     * MTR 在拿页 latch 前申请预算，commit/rollback 释放预算，避免多个前台都在低水位 begin 后集中 append。
     */
    private final AtomicLong reservedAppendBytes = new AtomicLong();

    /**
     * 创建 {@code RedoCapacityThrottle}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     */
    private RedoCapacityThrottle() {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        this.capacityPolicy = null;
        this.currentLsnSupplier = null;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.checkpointLsnSupplier = null;
        this.asyncFlushRequest = null;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.blockingFlushOnce = null;
        this.timeout = Duration.ZERO;
        this.noOp = true;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.maxPhysicalBatchBytes = Long.MAX_VALUE;
    }

    /**
     * 创建前台 redo capacity throttle。
     *
     * @param capacityPolicy checkpoint age 分级策略。
     * @param currentLsnSupplier 当前 redo LSN 来源。
     * @param checkpointLsnSupplier checkpoint/reclaim boundary 来源。
     * @param asyncFlushRequest ASYNC_FLUSH 下只发请求、不阻塞前台的动作。
     * @param blockingFlushOnce SYNC_FLUSH/HARD_LIMIT 下的一次 flush/checkpoint 推进动作。
     * @param timeout SYNC_FLUSH/HARD_LIMIT 最大等待时间，0 表示只尝试一次推进后立即按结果判定。
     */
    public RedoCapacityThrottle(RedoCapacityPolicy capacityPolicy,
                                Supplier<Lsn> currentLsnSupplier,
                                Supplier<Lsn> checkpointLsnSupplier,
                                Runnable asyncFlushRequest,
                                Runnable blockingFlushOnce,
                                Duration timeout) {
        this(capacityPolicy, currentLsnSupplier, checkpointLsnSupplier, asyncFlushRequest,
                blockingFlushOnce, timeout, Long.MAX_VALUE);
    }

    /**
     * 创建同时校验单批物理 file-fit 的生产 throttle。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param maxPhysicalBatchBytes repository 可原子写入一个 sealed batch 的最大物理字节。
     * @param capacityPolicy 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param currentLsnSupplier redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param checkpointLsnSupplier redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param asyncFlushRequest 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param blockingFlushOnce 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoCapacityThrottle(RedoCapacityPolicy capacityPolicy,
                                Supplier<Lsn> currentLsnSupplier,
                                Supplier<Lsn> checkpointLsnSupplier,
                                Runnable asyncFlushRequest,
                                Runnable blockingFlushOnce,
                                Duration timeout,
                                long maxPhysicalBatchBytes) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (capacityPolicy == null || currentLsnSupplier == null || checkpointLsnSupplier == null
                || asyncFlushRequest == null || blockingFlushOnce == null || timeout == null) {
            throw new DatabaseValidationException("redo capacity throttle dependencies must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("redo capacity throttle timeout must not be negative: " + timeout);
        }
        if (maxPhysicalBatchBytes <= 0) {
            throw new DatabaseValidationException("redo maximum physical batch bytes must be positive: "
                    + maxPhysicalBatchBytes);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.capacityPolicy = capacityPolicy;
        this.currentLsnSupplier = currentLsnSupplier;
        this.checkpointLsnSupplier = checkpointLsnSupplier;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.asyncFlushRequest = asyncFlushRequest;
        this.blockingFlushOnce = blockingFlushOnce;
        this.timeout = timeout;
        this.noOp = false;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.maxPhysicalBatchBytes = maxPhysicalBatchBytes;
    }

    /** 生产 capacity-aware throttle 是否要求每个 MTR 显式声明预算。 */
    public boolean requiresExplicitBudget() {
        return !noOp;
    }

    /**
     * 在 redo append/reservation 前执行容量反压。
     *
     * <p>数据流：读取 current/checkpoint → 评估 pressure → NONE 直接返回；ASYNC_FLUSH 只请求一次后台推进；
     * SYNC_FLUSH/HARD_LIMIT 循环执行推进并重新评估，直到压力降到 SYNC_FLUSH 以下或 timeout。HARD_LIMIT timeout
     * 仍以领域异常 fail-closed，避免让文件环覆盖未 checkpoint 的 redo。
     */
    public void beforeAppend() {
        beforeAppend(0);
    }

    /**
     * 在已知本次 append 预计字节数时执行容量反压。该方法不占用长期预算，适合非 MTR 的直接 redo append；
     * MTR 路径应优先使用 {@link #reserveAppendBytes(long)}，在拿页 latch 前把本次临界区的 redo 预算计入压力判断。
     *
     * @param appendBytes 本次 append 预计写入的 redo 字节数，必须非负。
     */
    public void beforeAppend(long appendBytes) {
        if (noOp) {
            return;
        }
        validateAppendBytes(appendBytes);
        waitUntilBelowSync(addExact(reservedAppendBytes.get(), appendBytes));
    }

    /**
     * 在前台 MTR 获取 page latch / buffer fix / FSP lease 前预留一段 redo 预算。
     *
     * <p>数据流：把当前 redo LSN、已授权预算和本次预算相加形成 prospective current LSN，再按 capacity policy
     * 做 log-free-check；ASYNC 只请求后台，SYNC/HARD 同步推进 flush/checkpoint。授权后的 reservation 挂入
     * MTR memo，随 commit/rollback 释放。该预算是教学实现的保守近似，不改变真实 LSN 分配；真实 LSN 仍由
     * {@link RedoLogManager#append(java.util.List)} 串行分配。
     *
     * @param appendBytes 本 MTR 预计最多产生的 redo 字节数，必须非负。
     * @return 必须随 MTR 生命周期关闭的预算句柄。
     */
    public Reservation reserveAppendBytes(long appendBytes) {
        return reserveAppendBudget(RedoAppendBudget.upperBound(RedoBudgetPurpose.TEST_UNBOUNDED, appendBytes));
    }

    /**
     * 在任何 page/FSP 资源获取前按操作预算执行 logical capacity 与 physical batch-fit 双重准入。
     * physical 超限无需 flush 即失败，因为 checkpoint 推进不能让单个 sealed batch 变小。
     *
     * @param budget redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code reserveAppendBudget} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoBudgetTooLargeException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public Reservation reserveAppendBudget(RedoAppendBudget budget) {
        if (noOp) {
            return Reservation.noOp();
        }
        if (budget == null) {
            throw new DatabaseValidationException("redo append budget must not be null");
        }
        if (budget.physicalBlockUpperBound() > maxPhysicalBatchBytes) {
            throw new RedoBudgetTooLargeException("redo budget cannot fit one repository batch: purpose="
                    + budget.purpose() + ", physical=" + budget.physicalBlockUpperBound()
                    + ", maxPhysical=" + maxPhysicalBatchBytes);
        }
        long appendBytes = budget.logicalUpperBound();
        // 只读 MTR 不消费 redo capacity；即使当前写流量已处于压力区，也不能让读路径代替写者触发 flush。
        if (appendBytes == 0) {
            return Reservation.noOp();
        }
        while (true) {
            long existing = reservedAppendBytes.get();
            long prospectiveBytes = addExact(existing, appendBytes);
            waitUntilBelowSync(prospectiveBytes);
            if (reservedAppendBytes.compareAndSet(existing, prospectiveBytes)) {
                return new Reservation(this, appendBytes);
            }
        }
    }

    private void waitUntilBelowSync(long prospectiveBytes) {
        RedoCapacityDecision decision = evaluate(prospectiveBytes);
        if (decision.pressure() == RedoCapacityPressure.NONE) {
            return;
        }
        if (decision.pressure() == RedoCapacityPressure.ASYNC_FLUSH) {
            asyncFlushRequest.run();
            return;
        }
        blockingFlushOnce.run();
        long deadline = deadlineFromNow(timeout);
        while (true) {
            decision = evaluate(prospectiveBytes);
            if (decision.pressure() == RedoCapacityPressure.NONE
                    || decision.pressure() == RedoCapacityPressure.ASYNC_FLUSH) {
                return;
            }
            if (deadlineReached(deadline)) {
                throw timeout(decision);
            }
            parkBriefly(deadline);
            blockingFlushOnce.run();
        }
    }

    private RedoCapacityDecision evaluate(long prospectiveBytes) {
        Lsn current = currentLsnSupplier.get();
        long currentValue = current.value();
        long prospectiveCurrent = addExact(currentValue, prospectiveBytes);
        return capacityPolicy.evaluate(Lsn.of(prospectiveCurrent), checkpointLsnSupplier.get());
    }

    private RedoCapacityThrottleTimeoutException timeout(RedoCapacityDecision decision) {
        return new RedoCapacityThrottleTimeoutException("redo capacity throttle timed out at pressure "
                + decision.pressure() + ": current=" + currentLsnSupplier.get().value()
                + ", checkpoint=" + checkpointLsnSupplier.get().value()
                + ", reserved=" + reservedAppendBytes.get());
    }

    /**
     * 释放本方法拥有的Redo/WAL资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void releaseReservation(long bytes) {
        if (bytes == 0) {
            return;
        }
        while (true) {
            long current = reservedAppendBytes.get();
            long next = current - bytes;
            if (next < 0) {
                throw new DatabaseValidationException("redo reservation release exceeds outstanding bytes: release="
                        + bytes + ", outstanding=" + current);
            }
            if (reservedAppendBytes.compareAndSet(current, next)) {
                return;
            }
        }
    }

    private static void validateAppendBytes(long appendBytes) {
        if (appendBytes < 0) {
            throw new DatabaseValidationException("redo append reservation bytes must not be negative: " + appendBytes);
        }
    }

    private static long addExact(long left, long right) {
        if (right > 0 && Long.MAX_VALUE - left < right) {
            throw new DatabaseValidationException("redo capacity prospective LSN overflows: left="
                    + left + ", right=" + right);
        }
        return left + right;
    }

    private static long deadlineFromNow(Duration timeout) {
        long nanos = timeout.toNanos();
        long now = System.nanoTime();
        if (Long.MAX_VALUE - now < nanos) {
            return Long.MAX_VALUE;
        }
        return now + nanos;
    }

    private static boolean deadlineReached(long deadline) {
        return deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0;
    }

    private static void parkBriefly(long deadline) {
        if (deadline == Long.MAX_VALUE) {
            LockSupport.parkNanos(1_000_000L);
            return;
        }
        long remaining = deadline - System.nanoTime();
        if (remaining > 0) {
            LockSupport.parkNanos(Math.min(remaining, 1_000_000L));
        }
    }

    /**
     * 前台 redo 预算句柄。句柄只负责释放 throttle 的内存预算，不代表真实 LSN 区间；真实 redo 区间仍在
     * MTR commit append 时分配。使用独立对象是为了让 MTR memo 用 try/finally/LIFO 统一释放，避免异常路径泄漏预算。
     */
    public static final class Reservation implements AutoCloseable {

        /**
         * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏Redo/WAL的不变量。
         */
        private static final Reservation NO_OP = new Reservation(null, 0);

        /** 拥有该预算的 throttle；no-op 句柄为空。 */
        private final RedoCapacityThrottle owner;
        /** 本句柄授权的 redo 预算字节数。 */
        private final long bytes;
        /** close 与 append ownership transfer 共用的一次性释放门，防止全局预算扣成负数。 */
        private final AtomicBoolean released = new AtomicBoolean();

        private Reservation(RedoCapacityThrottle owner, long bytes) {
            this.owner = owner;
            this.bytes = bytes;
        }

        private static Reservation noOp() {
            return NO_OP;
        }

        /**
         * append 成功分配真实 LSN 后立即把容量账本所有权转给 current LSN。此后 memo close 仍可调用但不会重复扣减。
         */
        public void transferToAppend() {
            releaseOnce();
        }

        /**
         * 释放本方法拥有的Redo/WAL资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         */
        @Override
        public void close() {
            releaseOnce();
        }

        private void releaseOnce() {
            if (owner != null && released.compareAndSet(false, true)) {
                owner.releaseReservation(bytes);
            }
        }
    }
}
