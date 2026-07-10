package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;

import java.util.Optional;

/**
 * 恢复 snapshot 中一条事务证据。
 *
 * @param transactionId 崩溃前写事务 id。
 * @param state 当前可证明的恢复状态。
 * @param transactionNo 提交号；未提交/已回滚可为 NONE。
 * @param terminalReason redo 记录原因；page3 合成证据为空。
 * @param source 当前状态的物理证据来源。
 * @param evidenceEndLsn 最新幂等证据所在 batch 的结束 LSN。
 */
public record RecoveredTransactionEntry(TransactionId transactionId,
                                        RecoveredTransactionState state,
                                        TransactionNo transactionNo,
                                        Optional<TransactionStateDeltaReason> terminalReason,
                                        RecoveredTransactionEvidenceSource source,
                                        Lsn evidenceEndLsn) {

    public RecoveredTransactionEntry {
        if (transactionId == null || state == null || transactionNo == null
                || terminalReason == null || source == null || evidenceEndLsn == null) {
            throw new DatabaseValidationException("recovered transaction entry fields must not be null");
        }
        if (transactionId.isNone()) {
            throw new DatabaseValidationException("recovered transaction entry requires a write transaction id");
        }
    }
}
