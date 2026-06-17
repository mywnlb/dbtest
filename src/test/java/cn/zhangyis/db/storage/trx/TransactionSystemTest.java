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
}
