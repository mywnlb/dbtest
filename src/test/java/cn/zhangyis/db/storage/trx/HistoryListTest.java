package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P5 HistoryList：committed 队列按提交序 FIFO。purge 先 peek，在 undo 段原子终结成功后再用
 * expected identity 精确完成队首；纯 insert undo 在提交路径直接终结，不再经过独立回收队列。
 */
class HistoryListTest {

    private static HistoryEntry entry(long no, long trx, long pageNo, int slot) {
        return new HistoryEntry(TransactionNo.of(no), TransactionId.of(trx), SpaceId.of(1),
                PageId.of(SpaceId.of(1), PageNo.of(pageNo)), UndoSlotId.of(slot));
    }

    @Test
    void committedIsFifoPeekThenCompleteExpectedHead() {
        HistoryList h = new HistoryList();
        HistoryEntry first = entry(1, 100, 65, 0);
        HistoryEntry second = entry(2, 101, 66, 1);
        h.submitCommitted(first);
        h.submitCommitted(second);
        assertEquals(2, h.committedSize());
        assertEquals(1L, h.peekCommitted().orElseThrow().transactionNo().value(), "peek 给最老提交，不移除");
        assertEquals(2, h.committedSize());
        h.completeCommitted(first);
        assertEquals(2L, h.peekCommitted().orElseThrow().transactionNo().value());
        h.completeCommitted(second);
        assertTrue(h.peekCommitted().isEmpty());
        assertEquals(0, h.committedSize());
    }

    @Test
    void completeRejectsWrongOrRepeatedHead() {
        HistoryList h = new HistoryList();
        HistoryEntry first = entry(1, 100, 65, 0);
        HistoryEntry second = entry(2, 101, 66, 1);
        h.submitCommitted(first);

        assertThrows(DatabaseValidationException.class, () -> h.completeCommitted(second),
                "错序完成不能摘除真实队首");
        assertEquals(first, h.peekCommitted().orElseThrow());
        h.completeCommitted(first);
        assertThrows(DatabaseValidationException.class, () -> h.completeCommitted(first),
                "重复完成不能静默成功");
    }
}
