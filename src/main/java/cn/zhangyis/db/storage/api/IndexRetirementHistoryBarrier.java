package cn.zhangyis.db.storage.api;

import java.time.Duration;

/**
 * Online DROP向事务/undo层请求的窄退休屏障。上层只传稳定 identity 和持久 fence 高水位，不读取活跃事务表、
 * history entry、undo page 或 purge lease。
 */
public interface IndexRetirementHistoryBarrier {

    /**
     * 捕获调用时刻最后一个已分配提交号，作为 final X 下的有限退休上界；本操作不消费事务号。
     *
     * @return 非负提交号；0 表示实例尚未分配任何 transaction number
     */
    long captureTransactionHighWater();

    /**
     * 等待可能引用退休索引且提交号不超过 fence 的 persistent history 全部退出。
     *
     * @param tableId final X 下重读的稳定正表 identity
     * @param indexId 待退休的稳定正二级索引 identity；实现可采用覆盖它的更保守表级投影
     * @param retireThroughTransactionNo fence 捕获的非负提交号高水位
     * @param timeout 最大等待时长，必须为正；等待期间不得持 page latch、MTR 或文件锁
     * @throws TablePurgeBarrierTimeoutException 超时仍有相关 history 时抛出，调用方必须保留物理 segment
     * @throws TablePurgeBarrierInterruptedException 等待被中断时抛出，调用方必须保留物理 segment
     */
    void awaitIndexHistorySafe(long tableId, long indexId,
                               long retireThroughTransactionNo, Duration timeout);
}
