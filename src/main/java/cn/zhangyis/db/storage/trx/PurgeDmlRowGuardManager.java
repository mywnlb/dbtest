package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DML 与 purge 的聚簇行短物理协调器。1024 个公平 {@link ReentrantLock} stripe 按 table id + 完整聚簇主键散列；
 * 相同 identity 必然进入同一 stripe，不同 identity 允许并发，但 hash collision 会保守串行而不影响正确性。
 *
 * <p>锁顺序：事务 record/unique lock -> 本 guard -> 单个 MTR/page latch。DML 只允许有界等待；purge 必须零等待，
 * busy 时把 task 留在 history 后重试。本协调器不进入 LockManager/Wait-For Graph，也不使用 Java monitor。</p>
 */
public final class PurgeDmlRowGuardManager {

    /** 固定 2 的幂 stripe 数；在内存占用和热点冲突之间取保守教学实现折中。 */
    private static final int STRIPE_COUNT = 1024;

    /** stripe mask；位与代替取模，前提由 STRIPE_COUNT 的 2 次幂不变量保证。 */
    private static final int STRIPE_MASK = STRIPE_COUNT - 1;

    /** 公平显式锁数组；每把锁只保护映射到该 stripe 的短物理行阶段，不保护事务生命周期。 */
    private final ReentrantLock[] stripes = new ReentrantLock[STRIPE_COUNT];

    /**
     * 创建全部公平 stripe；对象构造完成后数组引用及元素不再变化，可安全并发读取。
     */
    public PurgeDmlRowGuardManager() {
        for (int i = 0; i < stripes.length; i++) {
            stripes[i] = new ReentrantLock(true);
        }
    }

    /**
     * 为前台 DML 有界取得行 guard。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table/key/timeout，并在未接触共享锁前把 Duration 安全转换为纳秒。</li>
     *     <li>按 table id + 物化聚簇主键计算稳定 stripe；同线程嵌套获取被拒绝，避免 reentrant 掩盖生命周期错误。</li>
     *     <li>使用公平锁的 timed tryLock 等待；timeout 抛可回滚异常，期间不应持有 page latch/MTR。</li>
     *     <li>成功后返回 AutoCloseable guard；异常/中断路径没有锁所有权，中断会恢复线程标志。</li>
     * </ol>
     *
     * @param tableId    行所属表 identity；必须非负。
     * @param clusterKey 从物化聚簇行或待插入行提取的完整主键，不能使用未经类型归一化的用户字面量。
     * @param timeout    最大等待时长；允许零等待，不得为负。
     * @return 当前线程持有的短物理 guard。
     * @throws DatabaseValidationException table/key/timeout 无效、超出纳秒范围或同线程嵌套取得 stripe 时抛出。
     * @throws PurgeDmlRowGuardTimeoutException 在有界时间内未取得 stripe 时抛出；调用方可回滚当前 statement。
     * @throws PurgeDmlRowGuardInterruptedException 等待线程被中断时抛出；抛出前会恢复 interrupt flag。
     */
    public PurgeDmlRowGuard acquireForDml(long tableId, SearchKey clusterKey, Duration timeout) {
        // 1. 纯输入校验与时间换算先于共享锁访问，失败时没有释放义务。
        validateIdentity(tableId, clusterKey);
        if (timeout == null || timeout.isNegative()) {
            throw new DatabaseValidationException("DML row guard timeout must be non-null and non-negative");
        }
        final long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("DML row guard timeout exceeds nanosecond range", overflow);
        }

        // 2. identity 只用于定位 stripe；guard 不保存用户 key，避免延长大对象生命周期。
        ReentrantLock stripe = stripeFor(tableId, clusterKey);
        if (stripe.isHeldByCurrentThread()) {
            throw new DatabaseValidationException("nested DML row guard acquisition is not allowed for stripe");
        }

        // 3. 有界等待期间调用方不得持 page latch/MTR；timeout 明确返回 statement 可处理的领域异常。
        try {
            if (!stripe.tryLock(timeoutNanos, TimeUnit.NANOSECONDS)) {
                throw new PurgeDmlRowGuardTimeoutException("timed out acquiring DML row guard: table="
                        + tableId + ", key=" + clusterKey + ", timeout=" + timeout);
            }
        } catch (InterruptedException interrupted) {
            // 4. 中断路径没有取得锁；恢复标志并保留 cause，让上层终止 statement 而不是继续物理写。
            Thread.currentThread().interrupt();
            throw new PurgeDmlRowGuardInterruptedException(
                    "interrupted while acquiring DML row guard: table=" + tableId + ", key=" + clusterKey,
                    interrupted);
        }

        // 4. 成功后由 guard 的 try-with-resources 释放；manager 不隐藏 unlock 分支。
        return new PurgeDmlRowGuard(stripe);
    }

    /**
     * 为 purge 零等待尝试取得行 guard。busy（包括当前线程已持同一 stripe）立即返回 empty，不申请事务锁、
     * 不阻塞 history transition，也不产生异常；成功 guard 必须在单个 purge task 完成后立即关闭。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在接触共享 stripe 前校验表 id 与完整聚簇键；非法请求不改变任何锁状态。</li>
     *     <li>按稳定 identity 选择 stripe，并用零等待 tryLock 探测前台 DML 是否占用。</li>
     *     <li>busy 返回 empty，让 purge task 留在 history 重试；成功则把唯一 unlock 责任移交 guard。</li>
     * </ol>
     *
     * @param tableId    purge task 所属稳定表 id；必须非负。
     * @param clusterKey 从 exact-version 聚簇行/undo old image 重建的完整物化主键。
     * @return 成功时返回当前线程持有的短物理 guard；stripe busy 时返回 empty。
     * @throws DatabaseValidationException 表 id 无效或聚簇键为空时抛出。
     */
    public Optional<PurgeDmlRowGuard> tryAcquireForPurge(long tableId, SearchKey clusterKey) {
        // 1. 先校验 identity；失败路径尚未访问任何共享显式锁。
        validateIdentity(tableId, clusterKey);

        // 2. purge 只做零等待探测；同线程重入同样视作 busy，防止生命周期错误被可重入锁掩盖。
        ReentrantLock stripe = stripeFor(tableId, clusterKey);
        if (stripe.isHeldByCurrentThread() || !stripe.tryLock()) {
            // 3. busy 不改变 history 状态，调用方可在后续 purge batch 重试同一 task。
            return Optional.empty();
        }
        // 3. 成功路径把唯一释放责任交给 AutoCloseable guard。
        return Optional.of(new PurgeDmlRowGuard(stripe));
    }

    /**
     * 校验 row guard 的稳定 identity。
     *
     * @param tableId    行所属稳定表 id，必须非负。
     * @param clusterKey 已按聚簇类型系统物化的完整主键，不能为 {@code null}。
     * @throws DatabaseValidationException 任一 identity 字段无效时抛出。
     */
    private static void validateIdentity(long tableId, SearchKey clusterKey) {
        if (tableId < 0 || clusterKey == null) {
            throw new DatabaseValidationException("row guard table id/key must be valid");
        }
    }

    /**
     * 混合表 id 与归一化聚簇键 hash，定位固定公平 stripe。
     *
     * @param tableId    已通过校验的稳定表 id。
     * @param clusterKey 已通过校验的完整物化聚簇键。
     * @return 保护该 identity 短物理阶段的 stripe；hash collision 仅造成保守串行。
     */
    private ReentrantLock stripeFor(long tableId, SearchKey clusterKey) {
        int hash = 31 * Long.hashCode(tableId) + clusterKey.hashCode();
        hash ^= hash >>> 16;
        return stripes[hash & STRIPE_MASK];
    }
}
