package cn.zhangyis.db.storage.api.autoincrement;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.fsp.header.AutoIncrementHeader;
import cn.zhangyis.db.storage.fsp.header.AutoIncrementHeaderRepository;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 持久自增分配 Facade。每个 SpaceId 使用独立显式锁串行化“读 high-water→计算整批→提交页 0”；
 * 锁不覆盖普通记录 DML，页 latch 在 redo durable 等待前由 MTR commit 释放。
 */
public final class AutoIncrementService {

    private static final BigInteger MAX_HEADER_VALUE =
            BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    /** 短物理事务入口。 */
    private final MiniTransactionManager mtrManager;
    /** 页 0 字段仓储。 */
    private final AutoIncrementHeaderRepository headers;
    /** redo/data WAL 屏障。 */
    private final FlushService flush;
    /** 只按已访问 tablespace 增长；删除空间后遗留空锁不携带物理资源。 */
    private final ConcurrentHashMap<SpaceId, ReentrantLock> locks =
            new ConcurrentHashMap<>();

    /**
     * @param mtrManager 组合根 MTR 管理器
     * @param pool 组合根 Buffer Pool
     * @param flush WAL-safe flush 服务
     */
    public AutoIncrementService(
            MiniTransactionManager mtrManager, BufferPool pool,
            FlushService flush) {
        if (mtrManager == null || pool == null || flush == null) {
            throw new DatabaseValidationException(
                    "auto-increment service collaborators are required");
        }
        this.mtrManager = mtrManager;
        this.headers = new AutoIncrementHeaderRepository(pool);
        this.flush = flush;
    }

    /**
     * 解析一个 statement 的全部自增单元格并持久化最大已消耗值。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验列类型上限与全部显式值，在消费 high-water 前拒绝负数、零、列域越界和页 0 格式越界。</li>
     *     <li>在剩余 deadline 内取得 SpaceId 锁，再以页 0 X latch 读取 format/active/high-water。</li>
     *     <li>交叉校验持久 high-water 未超过列域，再按输入顺序为空请求生成 next，显式正值推进 high-water；以一个 MTR 写最终值并提交。</li>
     *     <li>MTR commit 已释放页 latch/fix 后等待 flushThrough；成功才发布结果，失败不回退已消耗值。</li>
     * </ol>
     *
     * @param spaceId 目标表的物理空间
     * @param requested 空元素表示生成，非空元素表示保留的显式正值
     * @param maximumValue AUTO_INCREMENT 列按 signed/unsigned 与整数位宽计算的正上限；不得超过页 0 可表达的 2^64-1
     * @param timeout 锁等待与 durable barrier 共用的正预算
     * @return 与请求同序的最终值及第一生成键
     * @throws DatabaseRuntimeException 锁超时、中断或持久化失败时抛出；调用方不得执行记录 DML
     * @throws DatabaseValidationException 请求、列上限、持久 high-water 或生成值超出列域时抛出；失败不修改页 0
     */
    public AutoIncrementAllocation allocate(
             SpaceId spaceId, List<Optional<BigInteger>> requested,
             BigInteger maximumValue, Duration timeout) {
        // 1、先验证无副作用输入，使无效批次绝不消耗序号。
        if (spaceId == null || requested == null || requested.isEmpty()
                || maximumValue == null || maximumValue.signum() <= 0
                || maximumValue.compareTo(MAX_HEADER_VALUE) > 0
                || timeout == null || timeout.isZero() || timeout.isNegative()
                || requested.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "auto-increment allocation request is invalid");
        }
        for (Optional<BigInteger> value : requested) {
            if (value.isPresent() && (value.orElseThrow().signum() <= 0
                    || value.orElseThrow().compareTo(maximumValue) > 0)) {
                throw new DatabaseValidationException(
                        "explicit auto-increment value is outside column range");
            }
        }

        long started = System.nanoTime();
        ReentrantLock lock = locks.computeIfAbsent(
                spaceId, ignored -> new ReentrantLock());
        boolean acquired;
        try {
            acquired = lock.tryLock(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException(
                    "auto-increment allocation interrupted for " + spaceId,
                    interrupted);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException(
                    "auto-increment timeout exceeds nanosecond range", overflow);
        }
        if (!acquired) {
            throw new DatabaseRuntimeException(
                    "auto-increment allocation timed out for " + spaceId);
        }

        try {
            // 2、SpaceId 锁先于 MTR/page latch，保证同一 high-water 只有一个计算者。
            MiniTransaction mtr = mtrManager.begin(
                    mtrManager.budgetFor(RedoBudgetPurpose.AUTO_INCREMENT));
            Lsn commitLsn;
            List<BigInteger> resolved;
            Optional<BigInteger> firstGenerated = Optional.empty();
            try {
                AutoIncrementHeader header =
                        headers.readForUpdate(mtr, spaceId);
                if (!header.active()) {
                    throw new DatabaseValidationException(
                            "DD requested AUTO_INCREMENT on inactive page0 header");
                }
                BigInteger highWater = unsigned(header.highWater());
                if (highWater.compareTo(maximumValue) > 0) {
                    throw new DatabaseValidationException(
                            "persisted AUTO_INCREMENT high-water exceeds column range");
                }
                resolved = new ArrayList<>(requested.size());
                // 3、整个批次在写页前完成顺序计算；任何溢出都只释放未修改 MTR。
                for (Optional<BigInteger> requestedValue : requested) {
                    BigInteger value;
                    if (requestedValue.isEmpty()) {
                        value = highWater.add(BigInteger.ONE);
                        if (value.compareTo(maximumValue) > 0) {
                            throw new DatabaseValidationException(
                                    "AUTO_INCREMENT column range exhausted");
                        }
                        if (firstGenerated.isEmpty()) {
                            firstGenerated = Optional.of(value);
                        }
                    } else {
                        value = requestedValue.orElseThrow();
                    }
                    if (value.compareTo(highWater) > 0) {
                        highWater = value;
                    }
                    resolved.add(value);
                }
                headers.writeHighWater(mtr, spaceId, highWater.longValue());
                commitLsn = mtrManager.commit(mtr);
            } catch (RuntimeException failure) {
                if (mtr.state() == MiniTransactionState.ACTIVE) {
                    mtrManager.rollbackUncommitted(mtr);
                }
                throw failure;
            }

            // 4、commit 已逆序释放 page guard；慢 WAL/flush 等待不持页 latch。
            Duration remaining = remaining(timeout, started);
            flush.flushThrough(commitLsn, remaining);
            return new AutoIncrementAllocation(resolved, firstGenerated);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 启动恢复完成后交叉校验 DD AUTO_INCREMENT 属性与 page0 active flag。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验空间身份；调用方必须只传入已由 committed DD discovery 打开的 tablespace。</li>
     *     <li>以短 MTR 读取并验证 page0 format/flags/reserved，期间不获取事务锁或用户记录锁。</li>
     *     <li>比较持久 active 与 exact DD 属性；不一致说明 DD/物理空间来自不同对象版本，必须 fail-closed。</li>
     *     <li>只读提交释放 page latch；失败回滚 MTR，不修改 high-water 或其它页状态。</li>
     * </ol>
     *
     * @param spaceId committed DD binding 指向的已打开表空间
     * @param expectedActive exact table metadata 是否声明 AUTO_INCREMENT
     * @throws DatabaseValidationException page0 格式损坏或 active 与 DD 不一致时抛出
     */
    public void validateMetadata(
            SpaceId spaceId, boolean expectedActive) {
        // 1、缺失 identity 不能转成系统空间或其它默认值继续读取。
        if (spaceId == null) {
            throw new DatabaseValidationException(
                    "AUTO_INCREMENT validation requires space identity");
        }
        // 2、复用唯一 header decoder，避免启动与发号对格式/保留位作不同解释。
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            AutoIncrementHeader header =
                    headers.readForUpdate(mtr, spaceId);
            // 3、不一致意味着 catalog/SDI 与 tablespace page0 不能共同授权用户写入。
            if (header.active() != expectedActive) {
                throw new DatabaseValidationException(
                        "DD and page0 AUTO_INCREMENT state mismatch for "
                                + spaceId);
            }
            // 4、只读 MTR 统一释放 memo；不产生 redo 或 dirty page。
            mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            if (mtr.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(mtr);
            }
            throw failure;
        }
    }

    private static BigInteger unsigned(long raw) {
        return new BigInteger(Long.toUnsignedString(raw));
    }

    private static Duration remaining(Duration timeout, long started) {
        long budget;
        try {
            budget = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            budget = Long.MAX_VALUE;
        }
        long elapsed = System.nanoTime() - started;
        long remaining = elapsed <= 0 ? budget
                : elapsed >= budget ? 0 : budget - elapsed;
        if (remaining == 0) {
            throw new DatabaseRuntimeException(
                    "auto-increment durable deadline expired");
        }
        return Duration.ofNanos(remaining);
    }
}
