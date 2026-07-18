package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;

/**
 * undo 固定前缀中无需 schema 即可读取的身份。rollback/purge 先据 tableId/indexId 选择元数据，再做完整 typed decode。
 *
 * @param type 选择 {@code 构造} 分支的 {@code UndoRecordType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param undoNo 参与 {@code 构造} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
 * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
 * @param indexId 参与 {@code 构造} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
 */
public record UndoRecordIdentity(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                                 long tableId, long indexId) {
    public UndoRecordIdentity {
        if (type == null || undoNo == null || transactionId == null || undoNo.isNone()
                || transactionId.isNone() || tableId <= 0 || indexId <= 0) {
            throw new DatabaseValidationException("invalid undo record identity");
        }
    }
}
