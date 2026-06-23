package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P5 HistoryList：committed 队列按提交序 FIFO（peek 不移除、poll 移除，供 purge per-entry 原子）；
 * insert-reclaim 队列可排空。onCommit→history 的路由由 P6 端到端测试覆盖。
 */
class HistoryListTest {

    private static HistoryEntry entry(long no, long trx, long pageNo, int slot) {
        return new HistoryEntry(TransactionNo.of(no), TransactionId.of(trx), SpaceId.of(1),
                PageId.of(SpaceId.of(1), PageNo.of(pageNo)), UndoSlotId.of(slot));
    }

    @Test
    void committedIsFifoPeekThenPoll() {
        HistoryList h = new HistoryList();
        h.submitCommitted(entry(1, 100, 65, 0));
        h.submitCommitted(entry(2, 101, 66, 1));
        assertEquals(2, h.committedSize());
        assertEquals(1L, h.peekCommitted().orElseThrow().transactionNo().value(), "peek 给最老提交，不移除");
        assertEquals(2, h.committedSize());
        assertEquals(1L, h.pollCommitted().orElseThrow().transactionNo().value());
        assertEquals(2L, h.pollCommitted().orElseThrow().transactionNo().value());
        assertTrue(h.peekCommitted().isEmpty());
        assertEquals(0, h.committedSize());
    }

    @Test
    void insertReclaimDrains() {
        HistoryList h = new HistoryList();
        h.submitInsertReclaim(new InsertReclaimEntry(SpaceId.of(1), PageId.of(SpaceId.of(1), PageNo.of(70))));
        h.submitInsertReclaim(new InsertReclaimEntry(SpaceId.of(1), PageId.of(SpaceId.of(1), PageNo.of(71))));
        assertEquals(2, h.insertReclaimSize());
        assertTrue(h.pollInsertReclaim().isPresent());
        assertTrue(h.pollInsertReclaim().isPresent());
        assertTrue(h.pollInsertReclaim().isEmpty());
        assertEquals(0, h.insertReclaimSize());
    }
}
