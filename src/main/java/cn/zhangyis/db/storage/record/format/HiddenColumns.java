package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 聚簇 conventional 记录的隐藏列值对象（DB_TRX_ID + DB_ROLL_PTR，innodb-record-design §6）。
 * 非聚簇记录无此对象（{@code LogicalRecord.hiddenColumns()==null}）。两字段均非空——空指针用
 * {@link RollPointer#NULL} 表达，而非 Java null。
 *
 * @param dbTrxId   最后写该记录的事务 id。
 * @param dbRollPtr 指向 undo record 的回滚指针；本片恒为 {@link RollPointer#NULL}（尚无 undo）。
 */
public record HiddenColumns(TransactionId dbTrxId, RollPointer dbRollPtr) {

    public HiddenColumns {
        if (dbTrxId == null || dbRollPtr == null) {
            throw new DatabaseValidationException("hidden columns dbTrxId/dbRollPtr must not be null");
        }
    }
}
