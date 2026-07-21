package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.XaId;

/**
 * Registry 扫描后发布的最新 branch 快照。
 *
 * @param sequence append-only 单调序号；用于诊断与检测回退
 * @param xid XA 分支身份
 * @param transactionId storage write transaction 身份
 * @param state 最新持久状态
 */
public record XaRegistryEntry(long sequence, XaId xid, TransactionId transactionId,
                              XaRegistryState state) {

    public XaRegistryEntry {
        if (sequence <= 0 || xid == null || transactionId == null
                || transactionId.isNone() || state == null) {
            throw new DatabaseValidationException("XA registry entry fields are invalid");
        }
    }
}
