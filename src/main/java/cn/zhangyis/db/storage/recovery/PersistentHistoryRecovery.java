package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.trx.HistoryEntry;
import cn.zhangyis.db.storage.undo.RollbackSegmentHistoryBase;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * page3 history base、occupied slot 证据与 undo first-page 双向链的恢复闭包校验器。
 *
 * <p>本类只编排不可变快照；每个节点如何在短 MTR 中读取由 {@link NodeReader} 注入。成功结果严格保持物理链顺序，
 * 不按 TransactionNo 排序。任何 cycle、断链、orphan COMMITTED、linked ACTIVE、重复 identity 或提交号高水位回退
 * 都是恢复安全性损坏，必须在开放流量前 fail-closed。
 */
public final class PersistentHistoryRecovery {

    /** 读取单个 first page 的无 latch 快照；生产实现保证一节点一短 MTR。 */
    @FunctionalInterface
    public interface NodeReader {
        UndoHistoryNodeSnapshot read(PageId firstPageId);
    }

    /**
     * 交叉校验并按 head→next 物理顺序重建运行时 history 条目。
     */
    public List<HistoryEntry> rebuild(RollbackSegmentHistoryBase base,
                                      Map<UndoSlotId, PageId> occupiedSlots,
                                      List<RecoveredUndoSlotEvidence> recoveredSlots,
                                      NodeReader nodeReader) {
        if (base == null || occupiedSlots == null || recoveredSlots == null || nodeReader == null) {
            throw new DatabaseValidationException("persistent history recovery inputs must not be null");
        }
        if (base.length() > occupiedSlots.size() || recoveredSlots.size() != occupiedSlots.size()) {
            throw new TransactionRecoveryException("history/slot evidence cardinality mismatch: history="
                    + base.length() + ", occupied=" + occupiedSlots.size()
                    + ", recovered=" + recoveredSlots.size());
        }
        Map<PageId, RecoveredUndoSlotEvidence> evidenceByPage = new HashMap<>();
        for (RecoveredUndoSlotEvidence evidence : recoveredSlots) {
            if (!evidence.firstPageId().equals(occupiedSlots.get(evidence.slotId()))
                    || evidenceByPage.put(evidence.firstPageId(), evidence) != null) {
                throw new TransactionRecoveryException("recovered undo slot/page mapping is not one-to-one: "
                        + evidence.firstPageId());
            }
        }
        Set<PageId> seenPages = new HashSet<>();
        Set<Long> seenCreators = new HashSet<>();
        Set<Long> seenCommitNos = new HashSet<>();
        List<HistoryEntry> physicalOrder = new ArrayList<>();
        Optional<PageId> current = base.headPageId();
        Optional<PageId> previous = Optional.empty();
        long maxCommitNo = 0L;
        for (long index = 0; index < base.length(); index++) {
            if (current.isEmpty()) {
                throw new TransactionRecoveryException("persistent history ended before declared length at " + index);
            }
            PageId pageId = current.orElseThrow();
            if (!seenPages.add(pageId)) {
                throw new TransactionRecoveryException("persistent history contains a cycle/duplicate node: " + pageId);
            }
            RecoveredUndoSlotEvidence evidence = evidenceByPage.get(pageId);
            if (evidence == null || evidence.state() != RecoveredUndoState.COMMITTED
                    || evidence.kind() != UndoLogKind.UPDATE) {
                throw new TransactionRecoveryException("history node is not a COMMITTED UPDATE occupied slot: "
                        + pageId);
            }
            UndoHistoryNodeSnapshot node = requireNode(nodeReader.read(pageId), pageId);
            if (!node.isCommitted() || node.kind() != UndoLogKind.UPDATE
                    || !node.creatorTransactionId().equals(evidence.creatorTransactionId())
                    || !node.committedTransactionNo().equals(evidence.transactionNo())
                    || !node.previousHistoryPageId().equals(previous)) {
                throw new TransactionRecoveryException("persistent history node/header/link mismatch: " + pageId);
            }
            if (!seenCreators.add(node.creatorTransactionId().value())
                    || !seenCommitNos.add(node.committedTransactionNo().value())) {
                throw new TransactionRecoveryException("persistent history creator/commit number is duplicated: "
                        + pageId);
            }
            maxCommitNo = Math.max(maxCommitNo, node.committedTransactionNo().value());
            physicalOrder.add(new HistoryEntry(node.committedTransactionNo(), node.creatorTransactionId(),
                    pageId.spaceId(), pageId, evidence.slotId()));
            previous = Optional.of(pageId);
            current = node.nextHistoryPageId();
        }
        if (current.isPresent() || !previous.equals(base.tailPageId())) {
            throw new TransactionRecoveryException("persistent history tail/declared length mismatch: base=" + base);
        }
        for (RecoveredUndoSlotEvidence evidence : recoveredSlots) {
            boolean linked = seenPages.contains(evidence.firstPageId());
            if (evidence.state() == RecoveredUndoState.COMMITTED && !linked) {
                throw new TransactionRecoveryException("COMMITTED occupied slot is omitted from history: "
                        + evidence.firstPageId());
            }
            if (evidence.state() == RecoveredUndoState.ACTIVE) {
                UndoHistoryNodeSnapshot active = requireNode(nodeReader.read(evidence.firstPageId()),
                        evidence.firstPageId());
                if (linked || !active.isActive() || active.kind() != evidence.kind()
                        || !active.creatorTransactionId().equals(evidence.creatorTransactionId())
                        || !active.committedTransactionNo().isNone()
                        || active.previousHistoryPageId().isPresent() || active.nextHistoryPageId().isPresent()) {
                    throw new TransactionRecoveryException("ACTIVE undo slot header/state must match evidence and "
                            + "remain unlinked: " + evidence.firstPageId());
                }
            }
        }
        if (base.lastTransactionNo().value() < maxCommitNo) {
            throw new TransactionRecoveryException("rseg lastTransactionNo is below live history maximum: last="
                    + base.lastTransactionNo().value() + ", max=" + maxCommitNo);
        }
        return List.copyOf(physicalOrder);
    }

    /**
     * 合并 transaction sidecar/redo 恢复出的 nextNo 与 page3 持久高水位。即使 history 已全部 purge，后者也不能
     * 丢失，否则重启会复用旧提交号。
     */
    public long nextTransactionNo(RollbackSegmentHistoryBase base, long reconciledNextTransactionNo) {
        if (base == null || reconciledNextTransactionNo < 1L) {
            throw new DatabaseValidationException("history counter recovery inputs are invalid");
        }
        final long historyNext;
        try {
            historyNext = Math.addExact(base.lastTransactionNo().value(), 1L);
        } catch (ArithmeticException overflow) {
            throw new TransactionRecoveryException("rseg lastTransactionNo cannot advance after recovery", overflow);
        }
        return Math.max(reconciledNextTransactionNo, historyNext);
    }

    private static UndoHistoryNodeSnapshot requireNode(UndoHistoryNodeSnapshot node, PageId expectedPageId) {
        if (node == null || !node.firstPageId().equals(expectedPageId)) {
            throw new TransactionRecoveryException("history node reader returned the wrong page: expected="
                    + expectedPageId + ", current=" + (node == null ? null : node.firstPageId()));
        }
        return node;
    }
}
