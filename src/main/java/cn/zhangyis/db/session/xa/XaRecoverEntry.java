package cn.zhangyis.db.session.xa;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.XaId;

/**
 * XA RECOVER 可公开的未决 branch，不包含 storage handle、页身份或运行期锁。
 *
 * @param xid 持久 PREPARED 的 XA 身份
 * @param transactionId 对应 storage transaction id，仅用于运维诊断
 */
public record XaRecoverEntry(XaId xid, long transactionId) {

    public XaRecoverEntry {
        if (xid == null || transactionId <= 0) {
            throw new DatabaseValidationException("XA recover entry fields are invalid");
        }
    }
}
