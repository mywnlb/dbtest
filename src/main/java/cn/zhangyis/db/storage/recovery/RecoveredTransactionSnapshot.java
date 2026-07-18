package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.Lsn;

import java.util.Map;
import java.util.Optional;

/**
 * redo replay 完成后的不可变事务恢复快照。它携带保守 next-counter 和按事务 id 索引的最新证据，供 page3
 * 交叉校验使用；发布后调用方不能修改恢复线程内部表。
 *
 * @param baselineCheckpointLsn sidecar 覆盖边界；完整 redo 尾必须至少到达这里。
 * @param baselineNextTransactionId sidecar 原始 id 高水位，用于判断被 checkpoint 过滤的 terminal 是否被覆盖。
 * @param baselineNextTransactionNo sidecar 原始提交号高水位。
 * @param nextTransactionId 合并 redo/page3 后下一次可分配事务 id 高水位。
 * @param nextTransactionNo 下一次可分配提交号高水位。
 * @param entries 事务证据不可变映射。
 */
public record RecoveredTransactionSnapshot(Lsn baselineCheckpointLsn,
                                           TransactionId baselineNextTransactionId,
                                           TransactionNo baselineNextTransactionNo,
                                           TransactionId nextTransactionId,
                                           TransactionNo nextTransactionNo,
                                           Map<TransactionId, RecoveredTransactionEntry> entries) {

    public RecoveredTransactionSnapshot {
        if (baselineCheckpointLsn == null || baselineNextTransactionId == null || baselineNextTransactionNo == null
                || nextTransactionId == null || nextTransactionNo == null || entries == null) {
            throw new DatabaseValidationException("recovered transaction snapshot fields must not be null");
        }
        if (baselineNextTransactionId.isNone() || baselineNextTransactionNo.isNone()
                || nextTransactionId.isNone() || nextTransactionNo.isNone()) {
            throw new DatabaseValidationException("recovered transaction snapshot counters must be positive");
        }
        entries = Map.copyOf(entries);
    }

    /** 按事务 id 查询证据；缺失表示 checkpoint 后没有 terminal/non-terminal delta。
     *
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @return {@code entry} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<RecoveredTransactionEntry> entry(TransactionId transactionId) {
        if (transactionId == null) {
            throw new DatabaseValidationException("recovered transaction lookup id must not be null");
        }
        return Optional.ofNullable(entries.get(transactionId));
    }
}
