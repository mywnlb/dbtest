package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.Optional;

/**
 * 一张 undo first page 的 history/recovery 不可变快照。读取过程只持首页 S latch，不跟随 segment tail，返回后可安全
 * 跨 MTR 使用；prev/next 表达跨事务 history 链，而 {@link UndoSegmentHandle} 的 first/last 表达段内 FIL 页链。
 * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
 * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
 * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
 * @param committedTransactionNo 参与 {@code 构造} 的稳定领域标识 {@code TransactionNo}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param previousHistoryPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
 * @param nextHistoryPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
 * @param logicalHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
 */
public record UndoHistoryNodeSnapshot(PageId firstPageId, UndoSegmentHandle handle,
                                      UndoLogKind kind, UndoLogState state,
                                      TransactionId creatorTransactionId,
                                      TransactionNo committedTransactionNo,
                                      Optional<PageId> previousHistoryPageId,
                                      Optional<PageId> nextHistoryPageId,
                                      UndoLogicalHead logicalHead) {

    public UndoHistoryNodeSnapshot {
        if (firstPageId == null || handle == null || kind == null || state == null || creatorTransactionId == null
                || committedTransactionNo == null || previousHistoryPageId == null
                || nextHistoryPageId == null || logicalHead == null) {
            throw new DatabaseValidationException("undo history node snapshot fields must not be null");
        }
        if (!firstPageId.equals(handle.firstPageId())) {
            throw new DatabaseValidationException("undo history snapshot first page/handle mismatch");
        }
        previousHistoryPageId.ifPresent(page -> requireSameSpace(firstPageId, page));
        nextHistoryPageId.ifPresent(page -> requireSameSpace(firstPageId, page));
    }

    public boolean isActive() {
        return state == UndoLogState.ACTIVE;
    }

    /** 当前 first page 是否是未决 XA participant，且仍不得进入 history。 */
    public boolean isPrepared() {
        return state == UndoLogState.PREPARED;
    }

    public boolean isCommitted() {
        return state == UndoLogState.COMMITTED;
    }

    public boolean isCached() {
        return state == UndoLogState.CACHED;
    }

    private static void requireSameSpace(PageId firstPageId, PageId linkedPageId) {
        if (!firstPageId.spaceId().equals(linkedPageId.spaceId())) {
            throw new DatabaseValidationException("undo history links must stay in one rollback segment space");
        }
    }
}
