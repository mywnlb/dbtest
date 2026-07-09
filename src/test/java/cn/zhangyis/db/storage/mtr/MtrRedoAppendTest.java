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
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.BTreePageDeltaKind;
import cn.zhangyis.db.storage.redo.BTreePageDeltaRecord;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.UndoRecordPayloadRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
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
    void commitClosesRedoRangeAfterDirtyPublish() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            mtr.getPage(pool, PID, PageLatchMode.EXCLUSIVE).writeBytes(120, new byte[]{4});

            Lsn committed = mgr.commit(mtr);

            assertEquals(committed, mgr.redoLogManager().closedLsn(),
                    "MTR commit must publish closed LSN after dirty page release");
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

    @Test
    void touchedPageBeforeSavepointStillStampsBatchEndLsn() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            PageId before = PageId.of(SPACE, PageNo.of(3));
            PageId after = PageId.of(SPACE, PageNo.of(4));
            mtr.getPage(pool, before, PageLatchMode.EXCLUSIVE).writeBytes(100, new byte[]{1});
            MtrSavepoint sp = mtr.savepoint();
            mtr.getPage(pool, after, PageLatchMode.SHARED);
            mtr.rollbackToSavepoint(sp);

            Lsn committed = mgr.commit(mtr);

            try (PageGuard g = pool.getPage(before, PageLatchMode.SHARED)) {
                assertEquals(committed, PageEnvelope.readPageLsn(g),
                        "savepoint 之前已 touched 的页仍要在 commit 时盖 batch end LSN");
            }
        });
    }

    @Test
    void readOnlyMtrDoesNotCloseExternalUnpublishedRedoRange() {
        onPool((pool, mgr) -> {
            mgr.redoLogManager().append(List.of(new PageBytesRecord(PID, 200, new byte[]{9})));
            assertEquals(0L, mgr.redoLogManager().closedLsn().value(), "外部 append 尚未 dirty publish");

            MiniTransaction readOnly = mgr.begin();
            readOnly.getPage(pool, PageId.of(SPACE, PageNo.of(4)), PageLatchMode.SHARED);
            mgr.commit(readOnly);

            assertEquals(0L, mgr.redoLogManager().closedLsn().value(),
                    "read-only MTR 的空 redo range 不能关闭前面未发布的 redo gap");
        });
    }

    @Test
    void multiPageCommitStampsSameBatchEndLsn() {
        onPool((pool, mgr) -> {
            PageId left = PageId.of(SPACE, PageNo.of(3));
            PageId right = PageId.of(SPACE, PageNo.of(4));
            MiniTransaction mtr = mgr.begin();
            mtr.getPage(pool, left, PageLatchMode.EXCLUSIVE).writeBytes(100, new byte[]{1});
            mtr.getPage(pool, right, PageLatchMode.EXCLUSIVE).writeBytes(100, new byte[]{2});

            Lsn committed = mgr.commit(mtr);

            try (PageGuard l = pool.getPage(left, PageLatchMode.SHARED);
                 PageGuard r = pool.getPage(right, PageLatchMode.SHARED)) {
                assertEquals(committed, PageEnvelope.readPageLsn(l));
                assertEquals(committed, PageEnvelope.readPageLsn(r));
            }
        });
    }

    @Test
    void redoEntriesTrackDefaultAndPageInitCategoriesWithoutChangingRecords() {
        onPool((pool, mgr) -> {
            PageId fresh = PageId.of(SPACE, PageNo.of(5));
            MiniTransaction mtr = mgr.begin();
            mtr.newPage(pool, fresh, PageLatchMode.EXCLUSIVE, PageType.INDEX).writeBytes(100, new byte[]{1});

            List<MtrRedoEntry> entries = mtr.redoEntries();

            assertIterableEquals(List.of(MtrRedoCategory.PAGE_INIT, MtrRedoCategory.PAGE_BYTES_GENERIC),
                    entries.stream().map(MtrRedoEntry::category).toList());
            assertTrue(entries.get(0).record() instanceof PageInitRecord);
            assertTrue(entries.get(1).record() instanceof PageBytesRecord);
            mgr.rollbackUncommitted(mtr);
        });
    }

    @Test
    void fspMetadataDeltaSuppressesCoveredPageBytesButStillStampsPage() {
        onPool((pool, mgr) -> {
            PageId page0 = PageId.of(SPACE, PageNo.of(0));
            MiniTransaction mtr = mgr.begin();
            PageGuard guard = mtr.getPage(pool, page0, PageLatchMode.EXCLUSIVE);

            FspRedoDeltas.writeLong(mtr, guard, page0, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                    0L, SpaceHeaderLayout.CURRENT_SIZE, SpaceHeaderLayout.CURRENT_SIZE, 128L,
                    "test FSP current size logical redo replacement");
            Lsn committed = mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertEquals(1, recs.size(), "covered FSP metadata bytes should be replaced by the logical delta");
            assertTrue(recs.get(0) instanceof FspMetadataDeltaRecord);
            assertFalse(recs.stream().anyMatch(PageBytesRecord.class::isInstance));
            try (PageGuard read = pool.getPage(page0, PageLatchMode.SHARED)) {
                assertEquals(128L, read.readLong(SpaceHeaderLayout.CURRENT_SIZE));
                assertEquals(committed, PageEnvelope.readPageLsn(read),
                        "metadata-only logical redo must still mark the page dirty at the batch end LSN");
            }
        });
    }

    @Test
    void undoMetadataDeltaSuppressesOnlyCoveredUndoPageBytes() {
        onPool((pool, mgr) -> {
            PageId undoPage = PageId.of(SPACE, PageNo.of(4));
            int metadataOffset = 128;
            int payloadOffset = 256;
            byte[] metadataImage = new byte[]{0, 0, 0, 42};
            byte[] payloadBytes = new byte[]{9, 8, 7};
            MiniTransaction mtr = mgr.begin();
            PageGuard guard = mtr.getPage(pool, undoPage, PageLatchMode.EXCLUSIVE);

            try (MtrRedoCategoryScope ignored =
                         mtr.enterRedoCategory(MtrRedoCategory.UNDO_PAGE_BYTES, "test undo metadata bytes")) {
                guard.writeBytes(metadataOffset, metadataImage);
                guard.writeBytes(payloadOffset, payloadBytes);
            }
            mtr.appendLogicalRedo(new UndoMetadataDeltaRecord(undoPage,
                            UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD, 0L, 0, metadataOffset, metadataImage),
                    MtrRedoCategory.UNDO_PAGE_BYTES, "test undo metadata logical redo replacement");

            Lsn committed = mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertEquals(2, recs.size(),
                    "covered undo metadata bytes should be replaced while record payload bytes stay physical");
            assertTrue(recs.stream().anyMatch(UndoMetadataDeltaRecord.class::isInstance));
            assertFalse(recs.stream().anyMatch(r -> r instanceof PageBytesRecord pbr
                    && pbr.pageId().equals(undoPage) && pbr.offset() == metadataOffset),
                    "metadata PAGE_BYTES covered by UndoMetadataDeltaRecord must be filtered");
            assertTrue(recs.stream().anyMatch(r -> r instanceof PageBytesRecord pbr
                    && pbr.pageId().equals(undoPage) && pbr.offset() == payloadOffset
                    && Arrays.equals(payloadBytes, pbr.bytes())),
                    "ordinary undo record payload bytes are not covered by metadata delta and must remain physical");
            try (PageGuard read = pool.getPage(undoPage, PageLatchMode.SHARED)) {
                assertArrayEquals(metadataImage, read.readBytes(metadataOffset, metadataImage.length));
                assertArrayEquals(payloadBytes, read.readBytes(payloadOffset, payloadBytes.length));
                assertEquals(committed, PageEnvelope.readPageLsn(read),
                        "metadata-only logical replacement still relies on the physical write to dirty/stamp page");
            }
        });
    }

    @Test
    void undoRecordPayloadDeltaSuppressesCoveredUndoPageBytes() {
        onPool((pool, mgr) -> {
            PageId undoPage = PageId.of(SPACE, PageNo.of(4));
            int recordOffset = 256;
            byte[] slotImage = new byte[]{0, 3, 9, 8, 7};
            MiniTransaction mtr = mgr.begin();
            PageGuard guard = mtr.getPage(pool, undoPage, PageLatchMode.EXCLUSIVE);

            try (MtrRedoCategoryScope ignored =
                         mtr.enterRedoCategory(MtrRedoCategory.UNDO_PAGE_BYTES, "test undo record payload bytes")) {
                guard.writeBytes(recordOffset, slotImage);
            }
            mtr.appendLogicalRedo(new UndoRecordPayloadRecord(undoPage,
                            cn.zhangyis.db.domain.TransactionId.of(11),
                            cn.zhangyis.db.domain.UndoNo.of(4), recordOffset, slotImage),
                    MtrRedoCategory.UNDO_PAGE_BYTES, "test undo record payload logical redo replacement");

            mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertEquals(1, recs.size(),
                    "undo payload logical redo should replace the covered physical slot bytes");
            assertTrue(recs.get(0) instanceof UndoRecordPayloadRecord);
            assertFalse(recs.stream().anyMatch(PageBytesRecord.class::isInstance));
        });
    }

    @Test
    void btreePageDeltaSuppressesCoveredStructureBytes() {
        onPool((pool, mgr) -> {
            PageId indexPage = PageId.of(SPACE, PageNo.of(4));
            int offset = PageEnvelopeLayout.PREV_PAGE_NO;
            byte[] links = new byte[]{0, 0, 0, 3, 0, 0, 0, 5};
            MiniTransaction mtr = mgr.begin();
            PageGuard guard = mtr.getPage(pool, indexPage, PageLatchMode.EXCLUSIVE);

            try (MtrRedoCategoryScope ignored =
                         mtr.enterRedoCategory(MtrRedoCategory.BTREE_STRUCTURE_BYTES, "test btree sibling bytes")) {
                guard.writeBytes(offset, links);
            }
            mtr.appendLogicalRedo(new BTreePageDeltaRecord(
                            indexPage, 7L, BTreePageDeltaKind.SIBLING_LINKS, 5L, offset, links),
                    MtrRedoCategory.BTREE_STRUCTURE_BYTES, "test btree sibling logical redo replacement");

            mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertEquals(1, recs.size(),
                    "B+Tree structure delta should replace the covered physical structure bytes");
            assertTrue(recs.get(0) instanceof BTreePageDeltaRecord);
            assertFalse(recs.stream().anyMatch(PageBytesRecord.class::isInstance));
        });
    }

    @Test
    void redoCategoryScopeIsNestedAndRestoredByLifo() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            PageGuard recordPage = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE);
            PageGuard btreePage = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(4)), PageLatchMode.EXCLUSIVE);
            PageGuard genericPage = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(5)), PageLatchMode.EXCLUSIVE);

            try (MtrRedoCategoryScope ignored =
                         mtr.enterRedoCategory(MtrRedoCategory.RECORD_PAGE_BYTES, "record insert bytes")) {
                recordPage.writeBytes(100, new byte[]{1});
                try (MtrRedoCategoryScope ignored2 =
                             mtr.enterRedoCategory(MtrRedoCategory.BTREE_STRUCTURE_BYTES, "btree split bytes")) {
                    btreePage.writeBytes(100, new byte[]{2});
                }
                recordPage.writeBytes(101, new byte[]{3});
            }
            genericPage.writeBytes(100, new byte[]{4});

            assertIterableEquals(List.of(
                            MtrRedoCategory.RECORD_PAGE_BYTES,
                            MtrRedoCategory.BTREE_STRUCTURE_BYTES,
                            MtrRedoCategory.RECORD_PAGE_BYTES,
                            MtrRedoCategory.PAGE_BYTES_GENERIC),
                    mtr.redoEntries().stream().map(MtrRedoEntry::category).toList());
            mgr.rollbackUncommitted(mtr);
        });
    }

    @Test
    void redoCategoryScopeOutOfOrderCloseDoesNotPoisonLaterLifoRecovery() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            PageGuard first = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE);
            PageGuard second = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(4)), PageLatchMode.EXCLUSIVE);
            PageGuard third = mtr.getPage(pool, PageId.of(SPACE, PageNo.of(5)), PageLatchMode.EXCLUSIVE);

            MtrRedoCategoryScope outer =
                    mtr.enterRedoCategory(MtrRedoCategory.RECORD_PAGE_BYTES, "record bytes");
            MtrRedoCategoryScope inner =
                    mtr.enterRedoCategory(MtrRedoCategory.BTREE_STRUCTURE_BYTES, "btree bytes");
            first.writeBytes(100, new byte[]{1});

            assertThrows(MtrStateException.class, outer::close);
            inner.close();
            outer.close();
            second.writeBytes(100, new byte[]{2});
            third.writeBytes(100, new byte[]{3});

            assertIterableEquals(List.of(
                            MtrRedoCategory.BTREE_STRUCTURE_BYTES,
                            MtrRedoCategory.PAGE_BYTES_GENERIC,
                            MtrRedoCategory.PAGE_BYTES_GENERIC),
                    mtr.redoEntries().stream().map(MtrRedoEntry::category).toList());
            mgr.rollbackUncommitted(mtr);
        });
    }
}
