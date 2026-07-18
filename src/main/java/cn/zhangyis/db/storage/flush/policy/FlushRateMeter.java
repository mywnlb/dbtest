package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.concurrent.locks.ReentrantLock;

/** 以显式锁保护的 flush 采样器；只累计最近一次采样窗口，不持有任何页或文件锁。 */
public final class FlushRateMeter {
    /**
     * 保护本对象共享状态的显式并发闩；获取后必须在 {@code finally} 或 Guard 关闭路径释放。
     */
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * 构造时冻结的 {@code previousRedo} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private Lsn previousRedo = Lsn.of(0);
    /**
     * 记录 {@code previousNanos} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private long previousNanos;
    /**
     * 记录 {@code previousFlushedPages} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private int previousFlushedPages;

    /**
     * 接收 {@code sample} 对应的脏页刷盘与 checkpoint生命周期事件；只更新本策略拥有的统计或顺序状态，不接管事件来源资源。
     *
     * @param currentRedo redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param flushedPages 参与 {@code sample} 的上界或规格值 {@code flushedPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param dirtyPages 参与 {@code sample} 的上界或规格值 {@code dirtyPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param capacityFrames 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param freeFrames 采样时 buffer pool 可立即分配的 frame 数；必须非负且不得超过实例总容量
     * @return {@code sample} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FlushRuntimeSnapshot sample(Lsn currentRedo, int flushedPages,
                                       int dirtyPages, int capacityFrames, int freeFrames) {
        if (currentRedo == null || flushedPages < 0) {
            throw new DatabaseValidationException("invalid flush rate sample");
        }
        long now = System.nanoTime();
        lock.lock();
        try {
            if (previousNanos == 0) {
                previousRedo = currentRedo;
                previousNanos = now;
                previousFlushedPages = flushedPages;
                return new FlushRuntimeSnapshot(0, 0, 0, dirtyPages, capacityFrames, freeFrames);
            }
            long redoDelta = Math.max(0L, currentRedo.value() - previousRedo.value());
            int flushedDelta = Math.max(0, flushedPages - previousFlushedPages);
            double seconds = Math.max(0.0, (now - previousNanos) / 1_000_000_000.0);
            previousRedo = currentRedo;
            previousNanos = now;
            previousFlushedPages = flushedPages;
            return new FlushRuntimeSnapshot(redoDelta, flushedDelta, seconds,
                    dirtyPages, capacityFrames, freeFrames);
        } finally {
            lock.unlock();
        }
    }
}
