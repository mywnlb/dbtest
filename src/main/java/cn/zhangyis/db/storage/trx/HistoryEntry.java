package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * History list 条目（设计 §5.6）：一条已提交、含 update/delete undo 的事务 undo log。purge 据 {@code transactionNo}
 * 判 boundary（{@code < purgeLowWaterNo} 可回收）、据 {@code creatorTrxId} 校验 delete-marked 记录所有权、经
 * {@code undoFirstPageId} 打开 undo 段遍历 DELETE_MARK 记录物理移除，全部成功后以 {@code slotId + firstPageId}
 * 作为 owner identity 原子 cache/free/drop 段并转移 page3 owner，最后发布内存 slot/reuse directory 状态。
 *
 * @param transactionNo   提交序号（FIFO/boundary 依据）。
 * @param creatorTrxId    写该 undo log 的事务 id（= delete-marked 记录应有的 DB_TRX_ID）。
 * @param undoSpaceId     undo 表空间。
 * @param undoFirstPageId undo 段链首页（purge 打开段、回收时取 handle 用）。
 * @param slotId          page3 与内存目录中的 rseg slot identity（purge 终结时做 owner CAS）。
 * @param affectedTableIds 当前 persistent logical chain 涉及的稳定表 id 集合；同一表在一条 history 中只计一次。
 */
public record HistoryEntry(TransactionNo transactionNo, TransactionId creatorTrxId, SpaceId undoSpaceId,
                           PageId undoFirstPageId, UndoSlotId slotId, Set<Long> affectedTableIds) {

    /**
     * 构造不携带 affected-table 投影的低层兼容条目；只允许用于不构造真实 logical chain 的测试。
     *
     * @param transactionNo   committed UPDATE undo log 的提交序号。
     * @param creatorTrxId    创建该 undo log 的写事务 id。
     * @param undoSpaceId     undo first page 所属表空间。
     * @param undoFirstPageId persistent history 链节点与 undo 段首页。
     * @param slotId          page3 rollback-segment slot identity。
     * @throws DatabaseValidationException 任一 identity 无效或 first page 不属于 undo space 时抛出。
     */
    public HistoryEntry(TransactionNo transactionNo, TransactionId creatorTrxId, SpaceId undoSpaceId,
                        PageId undoFirstPageId, UndoSlotId slotId) {
        this(transactionNo, creatorTrxId, undoSpaceId, undoFirstPageId, slotId, Set.of());
    }

    /**
     * 校验并冻结 persistent history 条目及其表引用投影。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验提交/创建者/undo owner identity，防止 purge 使用 NONE 或跨表空间 first page。</li>
     *     <li>校验 affected table id 为正数并排序去重，保证同一 history entry 对同一表只贡献一个 barrier 引用。</li>
     * </ol>
     *
     * @param transactionNo   非 NONE 的 committed transaction number。
     * @param creatorTrxId    非 NONE 的 undo creator transaction id。
     * @param undoSpaceId     与 first page space id 一致的 undo 表空间。
     * @param undoFirstPageId committed UPDATE undo 段首页与 history node identity。
     * @param slotId          page3 中当前持有该 first page 的 slot identity。
     * @param affectedTableIds 从 persistent logical chain 投影出的正 table id 集合；允许低层兼容空集合。
     * @throws DatabaseValidationException 字段缺失、使用 NONE、space 错配或表 id 非正时抛出。
     */
    public HistoryEntry {
        // 1. undo owner identity 必须完整且跨 page3/first-page/transaction 三方可交叉验证。
        if (transactionNo == null || creatorTrxId == null || undoSpaceId == null
                || undoFirstPageId == null || slotId == null || affectedTableIds == null) {
            throw new DatabaseValidationException("history entry fields must not be null");
        }
        if (transactionNo.isNone() || creatorTrxId.isNone()) {
            throw new DatabaseValidationException("history entry transaction no/id must not be NONE");
        }
        if (!undoSpaceId.equals(undoFirstPageId.spaceId())) {
            throw new DatabaseValidationException("history entry first page must belong to undo space: "
                    + undoFirstPageId);
        }
        // 2. 排序去重使运行时引用计数不依赖 HashSet 遍历顺序，同一 entry/表只计一次。
        TreeSet<Long> sorted = new TreeSet<>();
        for (Long tableId : affectedTableIds) {
            if (tableId == null || tableId <= 0L) {
                throw new DatabaseValidationException("history affected table ids must be positive");
            }
            sorted.add(tableId);
        }
        affectedTableIds = Collections.unmodifiableSet(new LinkedHashSet<>(sorted));
    }
}
