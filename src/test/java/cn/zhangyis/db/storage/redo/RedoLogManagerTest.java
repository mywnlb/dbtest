package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RedoLogManager：append 分配单调 LSN 区间、buffer 累积、空批退化、快照不可变。 */
class RedoLogManagerTest {

    private static final PageId PID = PageId.of(SpaceId.of(1), PageNo.of(3));

    @Test
    void appendAssignsContiguousMonotonicRanges() {
        RedoLogManager mgr = new RedoLogManager();
        PageBytesRecord a = new PageBytesRecord(PID, 0, new byte[]{1, 2, 3});
        PageInitRecord b = new PageInitRecord(PID, PageType.INDEX);

        LogRange r1 = mgr.append(List.of(a));
        LogRange r2 = mgr.append(List.of(b));

        assertEquals(0L, r1.start().value(), "first LSN at 0");
        assertEquals(r1.start().value() + a.byteLength(), r1.end().value());
        assertEquals(r1.end(), r2.start(), "ranges are contiguous");
        assertEquals(r1.end().value() + b.byteLength(), r2.end().value());
        assertEquals(r2.end(), mgr.currentLsn());
        assertEquals(2, mgr.bufferedRecords().size());
    }

    @Test
    void emptyBatchDoesNotAdvanceLsn() {
        RedoLogManager mgr = new RedoLogManager();
        LogRange r = mgr.append(List.of());
        assertEquals(r.start(), r.end(), "empty batch -> degenerate range");
        assertEquals(0, mgr.bufferedRecords().size());
    }

    @Test
    void bufferedRecordsIsImmutableSnapshot() {
        RedoLogManager mgr = new RedoLogManager();
        mgr.append(List.of(new PageInitRecord(PID, PageType.INDEX)));
        List<RedoRecord> snap = mgr.bufferedRecords();
        assertThrows(UnsupportedOperationException.class, () -> snap.add(new PageInitRecord(PID, PageType.INDEX)));
        assertTrue(snap.get(0) instanceof PageInitRecord);
    }

    /** 恢复完成后新 append 必须从 recoveredTo 继续，不能从 0 覆盖已有日志区间。 */
    @Test
    void restoreRecoveredBoundaryBeforeNewAppend() {
        RedoLogManager mgr = new RedoLogManager();
        mgr.restoreRecoveredBoundary(cn.zhangyis.db.domain.Lsn.of(100));

        LogRange range = mgr.append(List.of(new PageInitRecord(PID, PageType.INDEX)));

        assertEquals(100L, range.start().value());
        assertEquals(100L, mgr.flushedToDiskLsn().value());
        assertThrows(RuntimeException.class,
                () -> mgr.restoreRecoveredBoundary(cn.zhangyis.db.domain.Lsn.of(200)));
    }
}
