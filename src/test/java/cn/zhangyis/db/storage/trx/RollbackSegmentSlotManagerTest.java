package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3c 内存 rollback segment slot 目录。固定单一默认 {@link RollbackSegmentId}，slot array 记录
 * {@code UndoSlotId -> undoFirstPageId}，一把 {@link java.util.concurrent.locks.ReentrantLock} 串行
 * 「扫空槽→登记 firstPageId」，锁内不分配页、不访问 BufferPool、不等待 IO。
 *
 * <p>不测试 reload 持久性、slot 回收（非目标，留 T1.3d+）。
 */
class RollbackSegmentSlotManagerTest {

    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    private static PageId page(long pageNo) {
        return PageId.of(UNDO_SPACE, PageNo.of(pageNo));
    }

    @Test
    void freshManagerHasAllSlotsEmpty() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);

        assertEquals(0, mgr.activeSlotCount(), "no slots occupied on init");
        for (int i = 0; i < 8; i++) {
            assertFalse(mgr.isOccupied(UndoSlotId.of(i)), "slot " + i + " must be free on init");
        }
        assertEquals(RollbackSegmentId.of(0), mgr.rollbackSegmentId());
    }

    @Test
    void claimReturnsDistinctSlots() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);

        UndoSlotId s0 = mgr.claim(page(65));
        UndoSlotId s1 = mgr.claim(page(66));
        UndoSlotId s2 = mgr.claim(page(67));

        assertNotEquals(s0, s1);
        assertNotEquals(s0, s2);
        assertNotEquals(s1, s2);
        assertEquals(3, mgr.activeSlotCount());
    }

    @Test
    void claimedSlotPointsToInsertUndoFirstPage() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);

        UndoSlotId slot = mgr.claim(page(65));

        assertEquals(page(65), mgr.undoFirstPageId(slot));
        assertTrue(mgr.isOccupied(slot));
    }

    @Test
    void slotExhaustionThrowsDomainException() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 2);
        mgr.claim(page(65));
        mgr.claim(page(66));

        // 第 3 个 slot 超出容量 2，抛领域异常（UndoSlotExhaustedException extends DatabaseRuntimeException）
        DatabaseRuntimeException ex = assertThrows(DatabaseRuntimeException.class,
                () -> mgr.claim(page(67)));
        assertTrue(ex instanceof UndoSlotExhaustedException,
                "exhaustion must throw UndoSlotExhaustedException, got " + ex.getClass());
        assertEquals(2, mgr.activeSlotCount(), "exhaustion must not occupy a slot");
    }

    @Test
    void concurrentClaimProducesNoDuplicates() throws InterruptedException {
        int capacity = 32;
        int tasks = 100;
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), capacity);
        // 固定线程池小于任务数：任务排队执行但 claim 在 ReentrantLock 下短临界区，
        // 8 线程持续争用，足以验证「锁内扫空槽→登记」不产生重复 slot。不用 ready/start 栅栏
        // （poolSize < tasks 时栅栏会死锁：先调度的线程阻塞在 start.await() 占住线程，后续任务排不上）。
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<UndoSlotId> claimed = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger exhausted = new AtomicInteger();

        try {
            for (int i = 0; i < tasks; i++) {
                final PageId firstPage = page(100 + i);
                pool.submit(() -> {
                    try {
                        claimed.add(mgr.claim(firstPage));
                    } catch (UndoSlotExhaustedException ex) {
                        exhausted.incrementAndGet();
                    }
                });
            }
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS), "executor terminated");
        }

        // 成功认领的 slot 必须互不重复，且总数 == 容量；其余线程拿到耗尽异常
        Set<UndoSlotId> distinct = new HashSet<>(claimed);
        assertEquals(claimed.size(), distinct.size(), "no duplicate slot under concurrency");
        assertEquals(capacity, claimed.size(), "exactly capacity slots claimed");
        assertEquals(tasks - capacity, exhausted.get(), "remaining tasks hit exhaustion");
        assertEquals(capacity, mgr.activeSlotCount());
    }

    @Test
    void claimRejectsNullFirstPage() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);
        assertThrows(DatabaseRuntimeException.class, () -> mgr.claim(null));
    }

    /**
     * 槽位预留必须立即阻止并发认领；若 page3 预检或段创建尚未发生就退出，RAII close 应恢复 FREE。
     */
    @Test
    void unboundClaimLeaseReservesThenCancelsSlot() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 1);

        try (RollbackSegmentSlotManager.ClaimLease claim = mgr.reserveClaim()) {
            assertTrue(mgr.isOccupied(claim.slotId()), "RESERVED slot participates in occupancy");
            assertEquals(1, mgr.activeSlotCount(), "RESERVED slot prevents false empty diagnostics");
            assertThrows(UndoSlotExhaustedException.class, mgr::reserveClaim,
                    "a concurrent claim cannot reuse a RESERVED slot");
            assertThrows(DatabaseRuntimeException.class, () -> mgr.undoFirstPageId(claim.slotId()),
                    "RESERVED has no segment owner before bind");
        }

        assertEquals(0, mgr.activeSlotCount(), "closing an unbound claim cancels the reservation");
        assertFalse(mgr.isOccupied(UndoSlotId.of(0)));
    }

    /** 段创建完成后绑定 reservation，close 不得补偿释放已经进入 ACTIVE 的槽位。 */
    @Test
    void boundClaimLeasePublishesActiveOwner() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 1);
        UndoSlotId slot;

        try (RollbackSegmentSlotManager.ClaimLease claim = mgr.reserveClaim()) {
            slot = claim.slotId();
            claim.bind(page(65));
        }

        assertTrue(mgr.isOccupied(slot));
        assertEquals(page(65), mgr.undoFirstPageId(slot));
        assertEquals(1, mgr.activeSlotCount());
    }

    /** 预检失败发生在物理修改前时，终结租约 close 必须把 FINALIZING 恢复为 ACTIVE。 */
    @Test
    void finalizationLeaseRevertsToActiveBeforePhysicalMutation() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 1);
        UndoSlotId slot = mgr.claim(page(65));

        try (RollbackSegmentSlotManager.FinalizationLease ignored =
                     mgr.beginFinalization(slot, page(65))) {
            assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(slot, page(65)),
                    "duplicate terminal command must fail before touching physical pages");
        }

        assertEquals(page(65), mgr.undoFirstPageId(slot),
                "pre-physical failure keeps the original ACTIVE owner retryable");
    }

    /** MTR 已开始物理修改后即使退出，槽位也必须 fail-stop 保持占用，不能被新事务复用。 */
    @Test
    void finalizationLeaseRetainsSlotAfterPhysicalMutationBegins() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 1);
        UndoSlotId slot = mgr.claim(page(65));

        RollbackSegmentSlotManager.FinalizationLease lease = mgr.beginFinalization(slot, page(65));
        lease.physicalMutationStarted();
        lease.close();

        assertTrue(mgr.isOccupied(slot));
        assertEquals(1, mgr.activeSlotCount());
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(slot, page(65)));
        assertThrows(UndoSlotExhaustedException.class, mgr::reserveClaim,
                "a fail-stop FINALIZING slot cannot be reclaimed in the same process");
    }

    /** 只有持久 MTR 成功后显式 complete，终结租约才发布 FREE。 */
    @Test
    void completedFinalizationLeaseReleasesSlot() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 1);
        UndoSlotId slot = mgr.claim(page(65));

        try (RollbackSegmentSlotManager.FinalizationLease lease =
                     mgr.beginFinalization(slot, page(65))) {
            lease.physicalMutationStarted();
            lease.complete();
        }

        assertFalse(mgr.isOccupied(slot));
        assertEquals(0, mgr.activeSlotCount());
    }

    /** 已完成终结的槽被新 owner 复用后，旧命令必须因 expected owner 不符而在物理访问前失败。 */
    @Test
    void staleFinalizationCannotTouchReusedSlotOwner() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 1);
        UndoSlotId oldSlot = mgr.claim(page(65));
        completeFinalization(mgr, oldSlot, page(65));
        UndoSlotId reused = mgr.claim(page(66));

        assertEquals(oldSlot, reused, "first-fit reuses the completed slot");
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(oldSlot, page(65)));
        assertEquals(page(66), mgr.undoFirstPageId(reused), "stale command leaves the new owner untouched");
    }

    @Test
    void undoFirstPageIdRejectsUnoccupiedOrOutOfRange() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        // 未认领的 slot 查询必须失败，不能返回 stale/null 静默
        assertThrows(DatabaseRuntimeException.class, () -> mgr.undoFirstPageId(UndoSlotId.of(0)));
        // 越界 slot 查询失败
        assertThrows(DatabaseRuntimeException.class, () -> mgr.undoFirstPageId(UndoSlotId.of(99)));
    }

    // ---- T1.3d：slot 回收（commit/rollback 后释放，供后续事务重认领） ----

    @Test
    void completedFinalizationFreesSlotForReclaim() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 2);
        UndoSlotId s0 = mgr.claim(page(65));
        mgr.claim(page(66)); // 容量 2 占满

        completeFinalization(mgr, s0, page(65));

        // 释放后该 slot 空闲、可被新事务重认领（first-fit 复用最低空槽 = s0）
        assertFalse(mgr.isOccupied(s0), "released slot must be free");
        UndoSlotId reclaimed = mgr.claim(page(67));
        assertEquals(s0, reclaimed, "release makes the lowest slot reusable (first-fit)");
        assertEquals(page(67), mgr.undoFirstPageId(reclaimed), "reclaimed slot points to new first page");
    }

    @Test
    void completedFinalizationDropsActiveCount() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);
        UndoSlotId s0 = mgr.claim(page(65));
        UndoSlotId s1 = mgr.claim(page(66));
        assertEquals(2, mgr.activeSlotCount());

        completeFinalization(mgr, s0, page(65));
        assertEquals(1, mgr.activeSlotCount(), "release decrements active count");
        completeFinalization(mgr, s1, page(66));
        assertEquals(0, mgr.activeSlotCount(), "all released → empty");
    }

    @Test
    void finalizationOfUnoccupiedStaleOrNullOwnerThrows() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        // 终结未占用 slot 是调用方 bug（slot 生命周期不一致），必须抛领域异常不静默
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(UndoSlotId.of(0), page(65)));
        // 越界与 null 同样拒绝
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(UndoSlotId.of(99), page(65)));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(null, page(65)));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(UndoSlotId.of(0), null));
        // stale owner 与重复终结都拒绝
        UndoSlotId s = mgr.claim(page(65));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(s, page(66)),
                "finalization must compare the expected first-page owner");
        completeFinalization(mgr, s, page(65));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.beginFinalization(s, page(65)));
    }

    // ---- 0.3：恢复期 restore（扫 page3 rseg header 后按下标精确重建内存目录） ----

    @Test
    void restoreRepopulatesSlotByIndex() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);

        mgr.restore(UndoSlotId.of(3), page(70));

        assertTrue(mgr.isOccupied(UndoSlotId.of(3)));
        assertEquals(page(70), mgr.undoFirstPageId(UndoSlotId.of(3)));
        assertEquals(1, mgr.activeSlotCount());
        // 恢复后续认领走未占用空槽（first-fit 从 0），不影响已 restore 的 slot
        assertEquals(UndoSlotId.of(0), mgr.claim(page(71)));
    }

    @Test
    void restoreRejectsOccupiedOutOfRangeOrNull() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        mgr.restore(UndoSlotId.of(1), page(70));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.restore(UndoSlotId.of(1), page(71)));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.restore(UndoSlotId.of(99), page(72)));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.restore(UndoSlotId.of(2), null));
    }

    @Test
    void batchFinalizationCompletesAllSlotsAtomically() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        UndoSlotId insertSlot = mgr.claim(page(65));
        UndoSlotId updateSlot = mgr.claim(page(66));
        List<UndoLogBinding> bindings = List.of(
                new UndoLogBinding(UndoLogKind.INSERT, insertSlot, page(65), UndoLogicalHead.EMPTY),
                new UndoLogBinding(UndoLogKind.UPDATE, updateSlot, page(66), UndoLogicalHead.EMPTY));

        try (RollbackSegmentSlotManager.BatchFinalizationLease lease = mgr.beginBatchFinalization(bindings)) {
            lease.physicalMutationStarted();
            lease.complete();
        }

        assertEquals(0, mgr.activeSlotCount());
        assertFalse(mgr.isOccupied(insertSlot));
        assertFalse(mgr.isOccupied(updateSlot));
    }

    @Test
    void batchFinalizationBeforePhysicalMutationRestoresEveryOwner() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        UndoSlotId insertSlot = mgr.claim(page(65));
        UndoSlotId updateSlot = mgr.claim(page(66));
        List<UndoLogBinding> bindings = List.of(
                new UndoLogBinding(UndoLogKind.INSERT, insertSlot, page(65), UndoLogicalHead.EMPTY),
                new UndoLogBinding(UndoLogKind.UPDATE, updateSlot, page(66), UndoLogicalHead.EMPTY));

        try (RollbackSegmentSlotManager.BatchFinalizationLease ignored = mgr.beginBatchFinalization(bindings)) {
            // 物理写前退出必须恢复整批 ACTIVE。
        }

        assertEquals(2, mgr.activeSlotCount());
        assertEquals(page(65), mgr.undoFirstPageId(insertSlot));
        assertEquals(page(66), mgr.undoFirstPageId(updateSlot));
    }

    /** 测试侧按真实状态机发布一次已成功持久化的终结。 */
    private static void completeFinalization(RollbackSegmentSlotManager mgr, UndoSlotId slot, PageId owner) {
        try (RollbackSegmentSlotManager.FinalizationLease lease = mgr.beginFinalization(slot, owner)) {
            lease.physicalMutationStarted();
            lease.complete();
        }
    }
}
