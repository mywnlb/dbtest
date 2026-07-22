package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.IndexRetirementHistoryBarrier;

import java.time.Duration;

/**
 * 基于 {@link TransactionSystem} counter 与 {@link HistoryList} 权威队列实现的索引退休屏障。
 * 本对象不复制事务或 history 状态，也不持有第二把锁跨协作者调用。
 */
public final class HistoryIndexRetirementBarrier implements IndexRetirementHistoryBarrier {

    /** 提交号分配与恢复高水位的唯一 owner。 */
    private final TransactionSystem transactions;
    /** committed history、purge transition 与等待 Condition 的唯一 owner。 */
    private final HistoryList history;

    /**
     * 组合共享同一存储引擎实例的 counter 与 history；错误组合会让恢复 fence 失去意义，因此拒绝空依赖。
     *
     * @param transactions 当前实例的事务全局协调器
     * @param history 当前实例已由恢复重建、并由 purge worker消费的 history list
     * @throws DatabaseValidationException 任一依赖为空时抛出
     */
    public HistoryIndexRetirementBarrier(TransactionSystem transactions, HistoryList history) {
        if (transactions == null || history == null) {
            throw new DatabaseValidationException(
                    "index retirement barrier requires transaction system/history");
        }
        this.transactions = transactions;
        this.history = history;
    }

    /**
     * 在 TransactionSystem 短锁内读取 next transaction no，并转换为不消费号码的已分配高水位。
     *
     * @return {@code nextTransactionNo - 1}；新实例返回 0
     */
    @Override
    public long captureTransactionHighWater() {
        return transactions.snapshotCounters().nextTransactionNo().value() - 1L;
    }

    /**
     * 使用 table+transactionNo 的保守投影等待目标索引历史安全；index identity仍在入口校验，便于后续无损替换为
     * per-index persistent projection。调用期间不持 TransactionSystem lock。
     *
     * @param tableId retirement fence 中的稳定正表 identity
     * @param indexId retirement fence 中的稳定正二级索引 identity
     * @param retireThroughTransactionNo final X 捕获的非负提交号高水位
     * @param timeout 最大正等待时长
     * @throws DatabaseValidationException index id 非正时抛出
     * @throws cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException 高水位内 history 未及时清除时抛出
     * @throws cn.zhangyis.db.storage.api.TablePurgeBarrierInterruptedException 等待被中断时抛出
     */
    @Override
    public void awaitIndexHistorySafe(long tableId, long indexId,
                                      long retireThroughTransactionNo, Duration timeout) {
        if (indexId <= 0L) {
            throw new DatabaseValidationException(
                    "index retirement barrier requires a positive index id");
        }
        history.awaitTableUnreferencedThrough(tableId, retireThroughTransactionNo, timeout);
    }
}
