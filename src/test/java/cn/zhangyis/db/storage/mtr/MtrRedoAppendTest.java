package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MTR retrofit：写产 PAGE_BYTES、newPage 产 PAGE_INIT、commit 盖 pageLSN（不入 redo）、savepoint 拒绝释放 touched 页。 */
class MtrRedoAppendTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PID = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private interface Body { void run(BufferPool pool, MiniTransactionManager mgr); }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            body.run(pool, new MiniTransactionManager());
        }
    }

    @Test
    void writeProducesPageBytesAndStampsPageLsn() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            PageGuard g = mtr.getPage(pool, PID, PageLatchMode.EXCLUSIVE);
            g.writeBytes(100, new byte[]{1, 2, 3});
            Lsn committed = mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertEquals(1, recs.size());
            assertTrue(recs.get(0) instanceof PageBytesRecord);
            PageBytesRecord pb = (PageBytesRecord) recs.get(0);
            assertEquals(100, pb.offset());
            Lsn endLsn = mgr.redoLogManager().currentLsn();
            assertEquals(endLsn, committed);
            try (PageGuard g2 = pool.getPage(PID, PageLatchMode.SHARED)) {
                assertEquals(endLsn, PageEnvelope.readPageLsn(g2));
            }
            assertFalse(recs.stream().anyMatch(r -> r instanceof PageBytesRecord pbr
                    && pbr.offset() == PageEnvelopeLayout.PAGE_LSN), "pageLSN stamp must not be in redo");
        });
    }

    @Test
    void newPageProducesPageInitAndStamps() {
        onPool((pool, mgr) -> {
            PageId fresh = PageId.of(SPACE, PageNo.of(5));
            MiniTransaction mtr = mgr.begin();
            mtr.newPage(pool, fresh, PageLatchMode.EXCLUSIVE, PageType.INDEX);
            mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertTrue(recs.get(0) instanceof PageInitRecord, "newPage emits PAGE_INIT");
            assertEquals(PageType.INDEX, ((PageInitRecord) recs.get(0)).pageType());
            Lsn endLsn = mgr.redoLogManager().currentLsn();
            try (PageGuard g2 = pool.getPage(fresh, PageLatchMode.SHARED)) {
                assertEquals(endLsn, PageEnvelope.readPageLsn(g2), "PAGE_INIT page stamped even without extra write");
            }
        });
    }

    @Test
    void sharedOnlyPageProducesNoRedoNorStamp() {
        onPool((pool, mgr) -> {
            MiniTransaction w = mgr.begin();
            w.getPage(pool, PID, PageLatchMode.EXCLUSIVE).writeBytes(50, new byte[]{7});
            mgr.commit(w);
            Lsn afterWrite = mgr.redoLogManager().currentLsn();
            int recsAfterWrite = mgr.redoLogManager().bufferedRecords().size();

            MiniTransaction r = mgr.begin();
            r.getPage(pool, PID, PageLatchMode.SHARED);
            mgr.commit(r);

            assertEquals(recsAfterWrite, mgr.redoLogManager().bufferedRecords().size(), "S-only adds no redo");
            try (PageGuard g2 = pool.getPage(PID, PageLatchMode.SHARED)) {
                assertEquals(afterWrite, PageEnvelope.readPageLsn(g2), "S-only does not restamp");
            }
        });
    }

    @Test
    void rollbackToSavepointRejectsTouchedPage() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            MtrSavepoint sp = mtr.savepoint();
            PageGuard g = mtr.getPage(pool, PID, PageLatchMode.EXCLUSIVE);
            g.writeBytes(100, new byte[]{1}); // touched，且 guard 在保存点之上
            assertThrows(MtrStateException.class, () -> mtr.rollbackToSavepoint(sp));
            mgr.rollbackUncommitted(mtr); // 清理
        });
    }
}
