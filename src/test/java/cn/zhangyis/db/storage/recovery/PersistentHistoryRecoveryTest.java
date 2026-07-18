package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.RollbackSegmentHistoryBase;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogState;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 持久 history 恢复的闭包/损坏矩阵；节点读取用纯快照替代 IO，专注链与 slot 语义。 */
class PersistentHistoryRecoveryTest {

    private static final SpaceId SPACE = SpaceId.of(5);
    private static final PageId P1 = page(64);
    private static final PageId P2 = page(65);

    @Test
    void rebuildPreservesPhysicalOrderWhenTransactionNumbersAreLocallyInverted() {
        var fixture = twoCommitted(20, 10,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 20,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 10,
                        Optional.of(P1), Optional.empty()), TransactionNo.of(20));

        var rebuilt = new PersistentHistoryRecovery().rebuild(
                fixture.base(), fixture.owners(), fixture.evidence(), fixture.nodes()::get);

        assertEquals(List.of(P1, P2), rebuilt.stream().map(item -> item.undoFirstPageId()).toList());
        assertEquals(List.of(20L, 10L), rebuilt.stream()
                .map(item -> item.transactionNo().value()).toList());
    }

    /** recovery 不读取独立计数，而是逐 history node 的 persistent logical chain 重建 affected-table 集合。 */
    @Test
    void rebuildProjectsAffectedTablesFromPersistentLogicalChains() {
        var fixture = twoCommitted(20, 10,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 20,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 10,
                        Optional.of(P1), Optional.empty()), TransactionNo.of(20));

        var rebuilt = new PersistentHistoryRecovery().rebuild(
                fixture.base(), fixture.owners(), fixture.evidence(), fixture.nodes()::get,
                (pageId, logicalHead) -> pageId.equals(P1) ? java.util.Set.of(11L, 12L) : java.util.Set.of(12L));

        assertEquals(java.util.Set.of(11L, 12L), rebuilt.getFirst().affectedTableIds());
        assertEquals(java.util.Set.of(12L), rebuilt.getLast().affectedTableIds());
    }

    @Test
    void rejectsCyclePrevMismatchAndDeclaredTailMismatch() {
        PersistentHistoryRecovery recovery = new PersistentHistoryRecovery();
        Fixture cycle = twoCommitted(1, 2,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 1,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 2,
                        Optional.of(P1), Optional.of(P1)), TransactionNo.of(2));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(cycle.base(), cycle.owners(), cycle.evidence(), cycle.nodes()::get));

        Fixture badPrev = twoCommitted(1, 2,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 1,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 2,
                        Optional.empty(), Optional.empty()), TransactionNo.of(2));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(badPrev.base(), badPrev.owners(), badPrev.evidence(), badPrev.nodes()::get));

        RollbackSegmentHistoryBase wrongTail = new RollbackSegmentHistoryBase(
                Optional.of(P1), Optional.of(P1), 2, TransactionNo.of(2));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(wrongTail, badPrev.owners(), badPrev.evidence(), cycle.nodes()::get));
    }

    @Test
    void rejectsOrphanCommittedLinkedActiveAndNonUpdateNode() {
        PersistentHistoryRecovery recovery = new PersistentHistoryRecovery();
        Map<UndoSlotId, PageId> owners = Map.of(UndoSlotId.of(0), P1, UndoSlotId.of(1), P2);
        List<RecoveredUndoSlotEvidence> bothCommitted = List.of(
                RecoveredUndoSlotEvidence.committed(UndoSlotId.of(0), P1, UndoLogKind.UPDATE,
                        TransactionId.of(101), TransactionNo.of(1)),
                RecoveredUndoSlotEvidence.committed(UndoSlotId.of(1), P2, UndoLogKind.UPDATE,
                        TransactionId.of(102), TransactionNo.of(2)));
        RollbackSegmentHistoryBase onlyFirst = new RollbackSegmentHistoryBase(
                Optional.of(P1), Optional.of(P1), 1, TransactionNo.of(2));
        Map<PageId, UndoHistoryNodeSnapshot> orphanNodes = Map.of(
                P1, node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 1,
                        Optional.empty(), Optional.empty()),
                P2, node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 2,
                        Optional.empty(), Optional.empty()));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(onlyFirst, owners, bothCommitted, orphanNodes::get));

        List<RecoveredUndoSlotEvidence> activeEvidence = List.of(
                RecoveredUndoSlotEvidence.active(UndoSlotId.of(0), P1, UndoLogKind.UPDATE,
                        TransactionId.of(101)));
        Map<PageId, UndoHistoryNodeSnapshot> linkedActive = Map.of(P1,
                node(P1, UndoLogState.ACTIVE, UndoLogKind.UPDATE, 101, 0,
                        Optional.empty(), Optional.of(P2)));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(RollbackSegmentHistoryBase.empty(),
                        Map.of(UndoSlotId.of(0), P1), activeEvidence, linkedActive::get));

        Map<PageId, UndoHistoryNodeSnapshot> stateDriftedActive = Map.of(P1,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 1,
                        Optional.empty(), Optional.empty()));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(RollbackSegmentHistoryBase.empty(),
                        Map.of(UndoSlotId.of(0), P1), activeEvidence, stateDriftedActive::get),
                "ACTIVE slot 的二次节点快照必须保持 state/kind/creator/commitNo 一致");

        Fixture nonUpdate = twoCommitted(1, 2,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.INSERT, 101, 1,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 2,
                        Optional.of(P1), Optional.empty()), TransactionNo.of(2));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(nonUpdate.base(), nonUpdate.owners(), nonUpdate.evidence(),
                        nonUpdate.nodes()::get));
    }

    /**
     * PREPARED owner 与 ACTIVE 一样必须留在 history 链外，但 header 必须保持 PREPARED、creator/kind 一致且无提交号。
     */
    @Test
    void acceptsUnlinkedPreparedOwnerAndRejectsPreparedHeaderDrift() {
        PersistentHistoryRecovery recovery = new PersistentHistoryRecovery();
        List<RecoveredUndoSlotEvidence> evidence = List.of(
                RecoveredUndoSlotEvidence.prepared(
                        UndoSlotId.of(0), P1, UndoLogKind.UPDATE, TransactionId.of(101)));

        List<?> rebuilt = recovery.rebuild(
                RollbackSegmentHistoryBase.empty(),
                Map.of(UndoSlotId.of(0), P1),
                evidence,
                pageId -> node(pageId, UndoLogState.PREPARED, UndoLogKind.UPDATE,
                        101, 0, Optional.empty(), Optional.empty()));

        assertEquals(List.of(), rebuilt);
        assertThrows(TransactionRecoveryException.class, () -> recovery.rebuild(
                RollbackSegmentHistoryBase.empty(),
                Map.of(UndoSlotId.of(0), P1),
                evidence,
                pageId -> node(pageId, UndoLogState.ACTIVE, UndoLogKind.UPDATE,
                        101, 0, Optional.empty(), Optional.empty())));
    }

    @Test
    void rejectsDuplicateCreatorDuplicateCommitNumberAndLowHighWater() {
        PersistentHistoryRecovery recovery = new PersistentHistoryRecovery();
        Fixture duplicateCreator = twoCommitted(1, 2,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 1,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 2,
                        Optional.of(P1), Optional.empty()), TransactionNo.of(2));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(duplicateCreator.base(), duplicateCreator.owners(),
                        duplicateCreator.evidence(), duplicateCreator.nodes()::get));

        Fixture duplicateCommit = twoCommitted(1, 1,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 1,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 1,
                        Optional.of(P1), Optional.empty()), TransactionNo.of(1));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(duplicateCommit.base(), duplicateCommit.owners(),
                        duplicateCommit.evidence(), duplicateCommit.nodes()::get));

        Fixture lowHighWater = twoCommitted(3, 4,
                node(P1, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 101, 3,
                        Optional.empty(), Optional.of(P2)),
                node(P2, UndoLogState.COMMITTED, UndoLogKind.UPDATE, 102, 4,
                        Optional.of(P1), Optional.empty()), TransactionNo.of(3));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.rebuild(lowHighWater.base(), lowHighWater.owners(),
                        lowHighWater.evidence(), lowHighWater.nodes()::get));
    }

    @Test
    void allPurgedHistoryStillAdvancesTransactionNumberAndOverflowFailsClosed() {
        PersistentHistoryRecovery recovery = new PersistentHistoryRecovery();
        RollbackSegmentHistoryBase allPurged = new RollbackSegmentHistoryBase(
                Optional.empty(), Optional.empty(), 0, TransactionNo.of(20));
        assertEquals(21L, recovery.nextTransactionNo(allPurged, 5L));
        assertEquals(30L, recovery.nextTransactionNo(allPurged, 30L));
        RollbackSegmentHistoryBase exhausted = new RollbackSegmentHistoryBase(
                Optional.empty(), Optional.empty(), 0, TransactionNo.of(Long.MAX_VALUE));
        assertThrows(TransactionRecoveryException.class,
                () -> recovery.nextTransactionNo(exhausted, 1L));
    }

    private static Fixture twoCommitted(long firstNo, long secondNo,
                                        UndoHistoryNodeSnapshot firstNode,
                                        UndoHistoryNodeSnapshot secondNode,
                                        TransactionNo lastTransactionNo) {
        Map<UndoSlotId, PageId> owners = Map.of(UndoSlotId.of(0), P1, UndoSlotId.of(1), P2);
        List<RecoveredUndoSlotEvidence> evidence = List.of(
                RecoveredUndoSlotEvidence.committed(UndoSlotId.of(0), P1, UndoLogKind.UPDATE,
                        firstNode.creatorTransactionId(), TransactionNo.of(firstNo)),
                RecoveredUndoSlotEvidence.committed(UndoSlotId.of(1), P2, UndoLogKind.UPDATE,
                        secondNode.creatorTransactionId(), TransactionNo.of(secondNo)));
        RollbackSegmentHistoryBase base = new RollbackSegmentHistoryBase(
                Optional.of(P1), Optional.of(P2), 2, lastTransactionNo);
        return new Fixture(base, owners, evidence, Map.of(P1, firstNode, P2, secondNode));
    }

    private static UndoHistoryNodeSnapshot node(PageId pageId, UndoLogState state, UndoLogKind kind,
                                                long creator, long transactionNo,
                                                Optional<PageId> previous, Optional<PageId> next) {
        UndoSegmentHandle handle = new UndoSegmentHandle(SPACE, 0,
                SegmentId.of(pageId.pageNo().value()), pageId, pageId);
        return new UndoHistoryNodeSnapshot(pageId, handle, kind, state, TransactionId.of(creator),
                TransactionNo.of(transactionNo), previous, next, UndoLogicalHead.EMPTY);
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    private record Fixture(RollbackSegmentHistoryBase base, Map<UndoSlotId, PageId> owners,
                           List<RecoveredUndoSlotEvidence> evidence,
                           Map<PageId, UndoHistoryNodeSnapshot> nodes) {
    }
}
