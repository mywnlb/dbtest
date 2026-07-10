package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionSystem：id/no 单调、活跃表快照拷贝隔离。 */
class TransactionSystemTest {

    @Test
    void writeIdsAreMonotonic() {
        TransactionSystem sys = new TransactionSystem();
        TransactionId a = sys.allocateWriteId();
        TransactionId b = sys.allocateWriteId();
        assertTrue(b.value() > a.value());
    }

    @Test
    void transactionNosAreMonotonic() {
        TransactionSystem sys = new TransactionSystem();
        TransactionNo a = sys.allocateTransactionNo();
        TransactionNo b = sys.allocateTransactionNo();
        assertTrue(b.value() > a.value());
    }

    @Test
    void snapshotIsIsolatedCopy() {
        TransactionSystem sys = new TransactionSystem();
        TransactionId a = sys.allocateWriteId();
        Set<Long> snap = sys.snapshotActiveReadWriteIds();
        assertTrue(snap.contains(a.value()));
        // 拿快照后再分配，旧快照不受影响
        TransactionId b = sys.allocateWriteId();
        assertFalse(snap.contains(b.value()), "snapshot is an isolated copy");
        assertTrue(sys.snapshotActiveReadWriteIds().contains(b.value()));
    }

    @Test
    void removeActiveDropsId() {
        TransactionSystem sys = new TransactionSystem();
        TransactionId a = sys.allocateWriteId();
        sys.removeActive(a.value());
        assertEquals(Set.of(), sys.snapshotActiveReadWriteIds());
    }

    @Test
    void restoreCountersOnlyMovesForwardAndDrivesNextAllocations() {
        TransactionSystem sys = new TransactionSystem();

        sys.restoreCounters(10, 20);
        assertEquals(10L, sys.allocateWriteId().value(),
                "recovered TRANSACTION_ID high water determines the next write id");
        assertEquals(20L, sys.allocateTransactionNo().value(),
                "recovered COMMIT_NO high water determines the next transaction no");

        sys.restoreCounters(3, 4);
        assertEquals(11L, sys.allocateWriteId().value(),
                "restoreCounters must not move nextTransactionId backward");
        assertEquals(21L, sys.allocateTransactionNo().value(),
                "restoreCounters must not move nextTransactionNo backward");
    }

    /** checkpoint 快照读取的是两个 next-counter，不能消费号码或暴露活跃表可变状态。 */
    @Test
    void counterSnapshotCapturesNextValuesWithoutAllocatingThem() {
        TransactionSystem sys = new TransactionSystem();
        sys.allocateWriteId();
        sys.allocateTransactionNo();

        TransactionCounterSnapshot snapshot = sys.snapshotCounters();

        assertEquals(TransactionId.of(2), snapshot.nextTransactionId());
        assertEquals(TransactionNo.of(2), snapshot.nextTransactionNo());
        assertEquals(TransactionId.of(2), sys.allocateWriteId());
        assertEquals(TransactionNo.of(2), sys.allocateTransactionNo());
    }
}
