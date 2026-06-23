package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoSlotId;
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
 * {@code UndoSlotId -> insertUndoFirstPageId}，一把 {@link java.util.concurrent.locks.ReentrantLock} 串行
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

        assertEquals(page(65), mgr.insertUndoFirstPageId(slot));
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

    @Test
    void insertUndoFirstPageIdRejectsUnoccupiedOrOutOfRange() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        // 未认领的 slot 查询必须失败，不能返回 stale/null 静默
        assertThrows(DatabaseRuntimeException.class, () -> mgr.insertUndoFirstPageId(UndoSlotId.of(0)));
        // 越界 slot 查询失败
        assertThrows(DatabaseRuntimeException.class, () -> mgr.insertUndoFirstPageId(UndoSlotId.of(99)));
    }

    // ---- T1.3d：slot 回收（commit/rollback 后释放，供后续事务重认领） ----

    @Test
    void releaseFreesSlotForReclaim() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 2);
        UndoSlotId s0 = mgr.claim(page(65));
        mgr.claim(page(66)); // 容量 2 占满

        mgr.release(s0);

        // 释放后该 slot 空闲、可被新事务重认领（first-fit 复用最低空槽 = s0）
        assertFalse(mgr.isOccupied(s0), "released slot must be free");
        UndoSlotId reclaimed = mgr.claim(page(67));
        assertEquals(s0, reclaimed, "release makes the lowest slot reusable (first-fit)");
        assertEquals(page(67), mgr.insertUndoFirstPageId(reclaimed), "reclaimed slot points to new first page");
    }

    @Test
    void releaseDropsActiveCount() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 8);
        UndoSlotId s0 = mgr.claim(page(65));
        UndoSlotId s1 = mgr.claim(page(66));
        assertEquals(2, mgr.activeSlotCount());

        mgr.release(s0);
        assertEquals(1, mgr.activeSlotCount(), "release decrements active count");
        mgr.release(s1);
        assertEquals(0, mgr.activeSlotCount(), "all released → empty");
    }

    @Test
    void releaseOfUnoccupiedOrNullThrows() {
        RollbackSegmentSlotManager mgr = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), 4);
        // 释放未占用 slot 是调用方 bug（slot 生命周期不一致），必须抛领域异常不静默
        assertThrows(DatabaseRuntimeException.class, () -> mgr.release(UndoSlotId.of(0)));
        // 越界与 null 同样拒绝
        assertThrows(DatabaseRuntimeException.class, () -> mgr.release(UndoSlotId.of(99)));
        assertThrows(DatabaseRuntimeException.class, () -> mgr.release(null));
        // 重复释放：claim 后 release 两次，第二次抛
        UndoSlotId s = mgr.claim(page(65));
        mgr.release(s);
        assertThrows(DatabaseRuntimeException.class, () -> mgr.release(s));
    }
}
