package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageDeleter;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3d btree 物理删除 {@code deleteClustered}（rollback 反向走链消费 INSERT undo 的删除原语）。覆盖：
 * 匹配命中删除、未命中幂等、所有权（DB_TRX_ID/DB_ROLL_PTR）不匹配不删、已 delete-marked 直接 purge、
 * level-1 跨 leaf 删除、删后其余 key 仍可查。
 *
 * <p>所有权校验是关键不变量：rollback 只删本 undo 插入的行（dbTrxId+dbRollPtr 同时匹配），否则即便 cluster key
 * 相同也绝不误删（杜绝 orphan undo 场景下删掉别的事务后写入的同 key 行）。
 */
class BTreeDeleteClusteredTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(41);
    private static final long INDEX_ID = 9L;
    private static final long TRX = 123L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void matchingDeleteRemovesRecord() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rp = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(m);

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, index, kId(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(d);

            assertTrue(result.removed(), "matching record must be removed");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isEmpty(), "row gone after delete");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void missingKeyIsIdempotentNoOp() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();

            // 从未插入 key 7：orphan undo 兑现路径——找不到记录即 no-op，不抛
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, index, kId(7),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 7));
            ctx.mgr.commit(d);

            assertFalse(result.removed(), "missing key must yield removed=false, no exception");
        });
    }

    @Test
    void ownershipMismatchDoesNotDelete() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rp = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(m);

            // trx id 不符 → 不删
            MiniTransaction d1 = ctx.mgr.begin();
            assertFalse(svc.deleteClustered(d1, index, kId(1), TransactionId.of(999L), rp).removed(),
                    "different DB_TRX_ID must not delete");
            ctx.mgr.commit(d1);
            // roll pointer 不符 → 不删
            MiniTransaction d2 = ctx.mgr.begin();
            assertFalse(svc.deleteClustered(d2, index, kId(1), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 999)).removed(), "different DB_ROLL_PTR must not delete");
            ctx.mgr.commit(d2);

            // 记录仍在
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isPresent(), "record must survive non-matching deletes");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void alreadyDeleteMarkedGoesStraightToPurge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex index = ctx.clusteredIndex();
            RollPointer rp = new RollPointer(true, PageNo.of(65), 1);

            MiniTransaction m = ctx.mgr.begin();
            svc.insertClustered(m, index, row(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(m);

            // 手工把记录 delete-mark（模拟半失败/重试场景），deleteClustered 须跳过 mark 直接 purge
            MiniTransaction mark = ctx.mgr.begin();
            RecordPage leaf = ctx.access.openIndexPage(mark, ctx.rootPageId, PageLatchMode.EXCLUSIVE);
            OptionalInt off = new RecordPageSearch(registry).findEqual(leaf, kId(1), idKey(), clusteredSchema());
            assertTrue(off.isPresent(), "row must be present before manual delete-mark");
            new RecordPageDeleter().deleteMark(leaf, off.getAsInt());
            ctx.mgr.commit(mark);

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, index, kId(1), TransactionId.of(TRX), rp);
            ctx.mgr.commit(d);

            assertTrue(result.removed(), "already delete-marked matching record must still be purged");
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, index, kId(1)).isEmpty(), "row gone after purge");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void levelOneCrossLeafDeleteLeavesOthersQueryable() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = ctx.clusteredIndex();

            for (long id = 1; id <= 4; id++) {
                MiniTransaction m = ctx.mgr.begin();
                BTreeInsertResult res = svc.insertClustered(m, current, wideRow(id),
                        TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), (int) id));
                current = res.indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "4 wide rows split the root to level 1");

            // 删除一个非 root leaf 上的 key（id=4 落在右 leaf）
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult result = svc.deleteClustered(d, current, kId(4),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 4));
            ctx.mgr.commit(d);
            assertTrue(result.removed(), "cross-leaf delete must remove the row");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kId(4)).isEmpty(), "deleted key gone");
            assertTrue(svc.lookup(r, current, kId(1)).isPresent(), "other keys remain queryable");
            assertTrue(svc.lookup(r, current, kId(2)).isPresent());
            assertTrue(svc.lookup(r, current, kId(3)).isPresent());
            ctx.mgr.commit(r);
        });
    }

    @Test
    void multiLevelClusteredDeleteReplaceMarkPurge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            // 宽聚簇 KEY（5000B varchar 主键）→ node pointer ~5KB → 少量行即长出多层树（idKey 太小，root 容纳上百指针）。
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);

            long id = 1;
            while (current.rootLevel() < 2 && id <= 40) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }
            long inserted = id - 1;
            assertTrue(current.rootLevel() >= 2, "wide clustered key grows the tree past level 1");
            assertTrue(inserted >= 8, "enough rows span multiple leaves");

            // delete key 3（所有权 = 插入时 (TRX, rp3)）
            MiniTransaction d = ctx.mgr.begin();
            assertTrue(svc.deleteClustered(d, current, kKey(3), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 3)).removed(), "multi-level delete removes the row");
            ctx.mgr.commit(d);

            // replace key 5（同 key 换隐藏列；expected=插入版本）
            MiniTransaction rep = ctx.mgr.begin();
            assertTrue(svc.replaceClustered(rep, current, kKey(5),
                    wideKeyRowWithHidden(5, TransactionId.of(TRX), new RollPointer(true, PageNo.of(66), 105)),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 5)).replaced(),
                    "multi-level replace updates the matching row");
            ctx.mgr.commit(rep);

            // delete-mark key 7，再 purge
            MiniTransaction mk = ctx.mgr.begin();
            assertTrue(svc.setClusteredDeleteMark(mk, current, kKey(7), true,
                    new HiddenColumns(TransactionId.of(TRX), new RollPointer(true, PageNo.of(66), 207)),
                    TransactionId.of(TRX), new RollPointer(true, PageNo.of(65), 7)).changed(),
                    "multi-level delete-mark flips the matching row");
            ctx.mgr.commit(mk);

            MiniTransaction look = ctx.mgr.begin();
            assertTrue(svc.lookup(look, current, kKey(7)).isEmpty(), "delete-marked hidden from normal lookup");
            assertTrue(svc.lookupIncludingDeleted(look, current, kKey(7)).isPresent(), "still visible to MVCC read");
            ctx.mgr.commit(look);

            MiniTransaction pg = ctx.mgr.begin();
            assertTrue(svc.purgeDeleteMarkedClustered(pg, current, kKey(7), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(66), 207)).removed(), "multi-level purge physically removes");
            ctx.mgr.commit(pg);

            // 终态校验：删/purge 的不在、replace 的在、其余仍可查（多层导航全程正确）。
            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kKey(3)).isEmpty(), "deleted gone");
            assertTrue(svc.lookup(r, current, kKey(5)).isPresent(), "replaced present");
            assertTrue(svc.lookupIncludingDeleted(r, current, kKey(7)).isEmpty(), "purged gone");
            assertTrue(svc.lookup(r, current, kKey(1)).isPresent(), "untouched key present");
            assertTrue(svc.lookup(r, current, kKey(inserted)).isPresent(), "last key present");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void underfullLeafMergesIntoSiblingAndRemovesParentPointer() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            // 宽聚簇 KEY（5000B）→ 每 leaf ~2-3 行、node pointer ~5KB → 6 行长成 level-1 三叶树。
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long id = 1; id <= 6; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "6 wide-key rows form a level-1 tree");

            // 删最大 key（落在最右 leaf）使其欠载 → merge 进左兄弟、root 少一指针，但仍 ≥2 leaf（不整树 shrink）。
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(6), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 6));
            current = res.indexAfter();
            ctx.mgr.commit(d);

            assertTrue(res.removed(), "key 6 must be removed");
            assertFalse(res.freedPages().isEmpty(), "underfull leaf merged into sibling → victim page freed");
            assertEquals(1, current.rootLevel(), "merging two of three leaves keeps the tree at level 1");

            // 剩余 key 跨 sibling 链有序 scan 回（验证 merge 后链与 parent 结构完整）。
            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(6), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids, "remaining keys intact and ordered after merge");
        });
    }

    @Test
    void deleteAllRowsShrinksTreeToLevelZero() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            long id = 1;
            while (current.rootLevel() < 2 && id <= 40) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }
            long inserted = id - 1;
            assertTrue(current.rootLevel() >= 2, "multi-level tree before deletes");

            int totalFreed = 0;
            for (long k = 1; k <= inserted; k++) {
                MiniTransaction d = ctx.mgr.begin();
                BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(k), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) k));
                current = res.indexAfter();
                ctx.mgr.commit(d);
                assertTrue(res.removed(), "key " + k + " removed");
                totalFreed += res.freedPages().size();
            }

            assertEquals(0, current.rootLevel(), "deleting every row collapses the tree to a level-0 root leaf");
            assertEquals(ctx.rootPageId, current.rootPageId(), "root page id stays stable across shrink");
            assertTrue(totalFreed > 0, "merge/shrink freed interior + leaf pages");

            MiniTransaction r = ctx.mgr.begin();
            for (long k = 1; k <= inserted; k++) {
                assertTrue(svc.lookup(r, current, kKey(k)).isEmpty(), "key " + k + " gone after full delete");
            }
            ctx.mgr.commit(r);

            // 树仍可复用：collapse 后重新插入仍可查（root 页号稳定、空间回收后可再分配）。
            MiniTransaction m2 = ctx.mgr.begin();
            current = svc.insertClustered(m2, current, wideKeyRow(1), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 1)).indexAfterInsert();
            ctx.mgr.commit(m2);
            MiniTransaction r2 = ctx.mgr.begin();
            assertTrue(svc.lookup(r2, current, kKey(1)).isPresent(), "tree reusable after full shrink");
            ctx.mgr.commit(r2);
        });
    }

    @Test
    void internalUnderflowPropagatesAndShrinksRoot() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            long id = 1;
            while (current.rootLevel() < 2 && id <= 40) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
                id++;
            }
            long inserted = id - 1;
            int originalLevel = current.rootLevel();
            assertTrue(originalLevel >= 2, "start from a level-2 tree");

            // 删掉除最后两行外的全部 key：欠载沿内部层向上传播，root 至少 shrink 一层；剩余行仍可查。
            int totalFreed = 0;
            for (long k = 1; k <= inserted - 2; k++) {
                MiniTransaction d = ctx.mgr.begin();
                BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(k), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) k));
                current = res.indexAfter();
                ctx.mgr.commit(d);
                assertTrue(res.removed(), "key " + k + " removed");
                totalFreed += res.freedPages().size();
            }

            assertTrue(current.rootLevel() < originalLevel,
                    "internal-node merges propagate up and shrink the root at least one level");
            assertTrue(totalFreed > 0, "interior/leaf pages freed during propagation");
            assertEquals(ctx.rootPageId, current.rootPageId(), "root page id stable across propagation/shrink");

            MiniTransaction r = ctx.mgr.begin();
            assertTrue(svc.lookup(r, current, kKey(inserted - 1)).isPresent(), "kept key present");
            assertTrue(svc.lookup(r, current, kKey(inserted)).isPresent(), "kept key present");
            assertTrue(svc.lookup(r, current, kKey(1)).isEmpty(), "deleted key gone");
            ctx.mgr.commit(r);
        });
    }

    @Test
    void underfullLeafBorrowsFromFullSiblingViaRedistribute() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            // 1..5 → [1,2],[3,4,5]：右 leaf 满（3 行）。
            for (long id = 1; id <= 5; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "5 wide-key rows form a level-1 tree with a full right leaf");

            // 删最左 key 使左 leaf 欠载（1 行），同父右兄弟（满 3 行）容不下 merge → 改 redistribute 对半再平衡（0.12b）。
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(1), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 1));
            current = res.indexAfter();
            ctx.mgr.commit(d);

            assertTrue(res.removed(), "key 1 removed");
            assertTrue(res.freedPages().isEmpty(), "redistribute 不删页 → 无回收");
            assertEquals(1, current.rootLevel(), "redistribute 不改树高");

            // 白盒：两 leaf 记录数从「留欠载」的 1+3 再平衡为 2+2（这是 redistribute 而非 0.12 留欠载的判据）。
            assertEquals(List.of(2L, 2L), leafRecordCounts(ctx, current),
                    "redistribute rebalances the adjacent pair to ~half each");

            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(5), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(2L, 3L, 4L, 5L), ids, "remaining keys intact and ordered after redistribute");
        });
    }

    /**
     * 0.13a 乐观 delete safe 路径：多层树上删一条**删后不欠载**的记录，只在 leaf 持 X 完成、跳过 merge。
     * 1..5 → [1,2],[3,4,5]；删右 leaf 中间 key 4 → [3,5]（2 行不欠载）→ 乐观命中、不删页、剩余有序。
     */
    @Test
    void optimisticDeleteNonUnderfullSkipsMergeAndStaysCorrect() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long id = 1; id <= 5; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "5 wide-key rows form a level-1 tree with a full right leaf");

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(4), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 4));
            current = res.indexAfter();
            ctx.mgr.commit(d);

            assertTrue(res.removed(), "key 4 removed");
            assertTrue(res.freedPages().isEmpty(), "non-underfull delete must not merge/free any page");
            assertTrue(svc.optimisticDeleteHitCount() > 0, "non-underfull delete takes the optimistic leaf-only path");

            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(5), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(1L, 2L, 3L, 5L), ids, "remaining keys intact and ordered after optimistic delete");
        });
    }

    /**
     * 0.13a 乐观 delete unsafe 回退：删后欠载需 merge（须持全路径 X），乐观预判拦截、写页前释放 leaf X，改走悲观全 X。
     * 6 宽 key 行 → level-1 三叶树，删最大 key 使最右 leaf 欠载 → 回退悲观 → merge 进兄弟、victim 页回收。
     */
    @Test
    void optimisticDeleteUnderfullFallsBackToPessimisticMerge() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long id = 1; id <= 6; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(1, current.rootLevel(), "6 wide-key rows form a level-1 tree");

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(6), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 6));
            current = res.indexAfter();
            ctx.mgr.commit(d);

            assertTrue(res.removed(), "key 6 removed");
            assertFalse(res.freedPages().isEmpty(), "underfull delete falls back and merges → victim page freed");
            assertTrue(svc.pessimisticDeleteFallbackCount() > 0, "underfull delete takes the pessimistic fallback");

            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(6), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids, "remaining keys intact and ordered after fallback merge");
        });
    }

    /**
     * 0.13a 乐观 delete 幂等：多层树上删一个**不存在**的 key，经乐观 S-crab 下降到 leaf、findEqual 空 → no-op，
     * 纯读不改任何页（safe），不回退悲观。
     */
    @Test
    void optimisticDeleteMissOnMultiLevelIsNoOp() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long id = 1; id <= 5; id++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(id), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) id)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertTrue(current.rootLevel() >= 1, "multi-level tree");

            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(999), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 999));
            ctx.mgr.commit(d);

            assertFalse(res.removed(), "missing key on multi-level tree is an idempotent no-op");
            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(5), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), ids, "all keys intact after no-op delete");
        });
    }

    /**
     * 0.13d delete/purge safe-node（设计 §10.2 step4-5）：悲观 merge 下降时，一旦 latch 到「移除一个 pointer 后仍不欠载」
     * 的 safe 内部节点，立即释放其以上全部祖先 X（含 root）。10 宽 key 行 → level-2 树（root 子 [A=2ptr, B=3ptr]）：
     * B 满 3 指针（free≈1KB）→ 移除 1 指针后仍不欠载 = delete-safe；删 key 10 使 B 下最右 leaf 欠载 → 悲观回退下降
     * 经过 B（safe）→ 提前释放 root X → merge 只发生在 B 子树内（B 3→2 ptr）、root 不受影响、树高不变。
     *
     * <p>RED（未接 delete safe-node 前）：悲观 delete 全路径 X 持到 commit、从不早释放祖先，计数恒 0。
     */
    @Test
    void pessimisticMergeReleasesRootLatchEarlyWhenMergeDoesNotReachRoot() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            for (long n = 1; n <= 10; n++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(n), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) n)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(2, current.rootLevel(), "10 wide-key rows form a level-2 tree");
            List<Integer> fills = new ArrayList<>();
            collectFills(ctx, current, fills, new ArrayList<>());
            assertEquals(List.of(2, 3), fills, "root has a 2-ptr left and a full 3-ptr right level-1 child");

            // 删最大 key：B 下最右 leaf 欠载 → 乐观预判回退悲观 → 下降经过 delete-safe 的 B → 早释放 root（SX 首遍）。
            long sxBefore = svc.rootSxDescentCount();
            long restartBefore = svc.rootXRestartCount();
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(10), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 10));
            current = res.indexAfter();
            ctx.mgr.commit(d);

            assertTrue(res.removed(), "key 10 removed");
            assertFalse(res.freedPages().isEmpty(), "underfull rightmost leaf merged under B → victim freed");
            assertTrue(svc.pessimisticDeleteFallbackCount() > 0, "underfull delete takes the pessimistic fallback");
            assertTrue(svc.safeNodeDeleteAncestorReleaseCount() > 0,
                    "safe-node must release root latch early when the merge does not propagate to root");
            // 0.13d SX+restart：快照 level 2 的悲观 merge 下降以 root SX 起步；B delete-safe → 不达 root → 零重启。
            assertTrue(svc.rootSxDescentCount() > sxBefore,
                    "snapshot-level>=2 pessimistic merge descends with the root SX first pass");
            assertEquals(restartBefore, svc.rootXRestartCount(),
                    "a merge absorbed below root must not restart with root X");
            assertEquals(2, current.rootLevel(), "merge stays inside B's subtree; tree height unchanged");

            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(10), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), ids,
                    "remaining keys intact and ordered after safe-node merge");
        });
    }

    // ---- helpers ----

    /** scan 结果取 payload 列（column1 = int v = id）。 */
    private static long vOf(BTreeLookupResult row) {
        return ((ColumnValue.IntValue) row.record().columnValues().get(1)).value();
    }

    @Test
    void internalRedistributeKeepsSubtreesBalanced() {
        onPool(ctx -> {
            ctx.createTablespaceAndRoot();
            SplitCapableBTreeIndexService svc = ctx.service();
            BTreeIndex current = new BTreeIndex(INDEX_ID, ctx.rootPageId, 0, wideKeyDef(), wideKeySchema(), true,
                    ctx.leafSegment, ctx.nonLeafSegment);
            // 10 宽 key 行 → level-2 树：root 两个 level-1 子，左 A=2 ptr、右 B=3 ptr（满）。
            for (long n = 1; n <= 10; n++) {
                MiniTransaction m = ctx.mgr.begin();
                current = svc.insertClustered(m, current, wideKeyRow(n), TransactionId.of(TRX),
                        new RollPointer(true, PageNo.of(65), (int) n)).indexAfterInsert();
                ctx.mgr.commit(m);
            }
            assertEquals(2, current.rootLevel(), "10 wide-key rows form a level-2 tree");
            List<Integer> before = new ArrayList<>();
            collectFills(ctx, current, before, new ArrayList<>());
            assertEquals(List.of(2, 3), before, "root has a 2-ptr left and a full 3-ptr right level-1 child");

            // 删最小 key → 左 A 一个 leaf 欠载并 merge → A 降到 1 ptr 欠载 → 与满兄弟 B(3 ptr) 合计 4>3 fit 不下
            // → 触发 internal redistribute 平分为 2+2（而非 0.12 留 1-ptr 退化内部页或 shrink）。
            MiniTransaction d = ctx.mgr.begin();
            BTreeDeleteResult res = svc.deleteClustered(d, current, kKey(1), TransactionId.of(TRX),
                    new RollPointer(true, PageNo.of(65), 1));
            current = res.indexAfter();
            ctx.mgr.commit(d);
            assertTrue(res.removed(), "key 1 removed");

            List<Integer> after = new ArrayList<>();
            collectFills(ctx, current, after, new ArrayList<>());
            assertEquals(2, current.rootLevel(), "internal redistribute keeps the tree at level 2 (no shrink)");
            assertEquals(List.of(2, 2), after,
                    "internal redistribute rebalances the two level-1 children to 2+2 (no 1-ptr degenerate page)");

            MiniTransaction r = ctx.mgr.begin();
            List<Long> ids = svc.scan(r, current, new BTreeScanRange(kKey(1), true, kKey(10), true, 50))
                    .stream().map(BTreeDeleteClusteredTest::vOf).toList();
            ctx.mgr.commit(r);
            assertEquals(List.of(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), ids, "remaining keys intact and ordered");
        });
    }

    /** 遍历整树，收集非根内部页的 pointer 数与 leaf 的记录数（白盒结构检查）。 */
    private void collectFills(Ctx ctx, BTreeIndex index, List<Integer> internalNonRoot, List<Integer> leaves) {
        MiniTransaction r = ctx.mgr.begin();
        try {
            walk(ctx, index, index.rootPageId(), index.rootLevel(), true, internalNonRoot, leaves, r);
            ctx.mgr.commit(r);
        } catch (Throwable t) {
            ctx.mgr.rollbackUncommitted(r);
            throw t;
        }
    }

    private void walk(Ctx ctx, BTreeIndex index, PageId pageId, int level, boolean isRoot,
                      List<Integer> internalNonRoot, List<Integer> leaves, MiniTransaction r) {
        RecordPage page;
        try (var ignored = r.allowOutOfOrderPageLatch(
                "btree test structural walk follows child pointers while retaining ancestors")) {
            page = ctx.access.openIndexPage(r, pageId, PageLatchMode.SHARED);
        }
        if (level == 0) {
            leaves.add(page.header().nRecs());
            return;
        }
        if (!isRoot) {
            internalNonRoot.add(page.header().nRecs());
        }
        BTreeNodePointerSchema ps = BTreeNodePointerSchema.from(index);
        BTreeNodePointerCodec codec = new BTreeNodePointerCodec();
        List<PageId> children = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            children.add(codec.fromRecord(
                    new RecordCursor(page, off, ps.schema(), registry).materialize(), ps).childPageId());
        }
        for (PageId c : children) {
            walk(ctx, index, c, level - 1, false, internalNonRoot, leaves, r);
        }
    }

    /** 白盒读 level-1 树各 leaf 的用户记录数（按 root pointer 顺序），验证 redistribute 后相邻对均衡而非 1+3 留欠载。 */
    private List<Long> leafRecordCounts(Ctx ctx, BTreeIndex index) {
        MiniTransaction r = ctx.mgr.begin();
        List<Long> counts = new ArrayList<>();
        BTreeNodePointerSchema ps = BTreeNodePointerSchema.from(index);
        BTreeNodePointerCodec codec = new BTreeNodePointerCodec();
        try {
            RecordPage root;
            try (var ignored = r.allowOutOfOrderPageLatch(
                    "btree test leaf fill walk follows root pointers while retaining root")) {
                root = ctx.access.openIndexPage(r, index.rootPageId(), PageLatchMode.SHARED);
            }
            for (int off : root.recordOffsetsInOrder()) {
                BTreeNodePointer p = codec.fromRecord(
                        new RecordCursor(root, off, ps.schema(), registry).materialize(), ps);
                RecordPage leaf;
                try (var ignored = r.allowOutOfOrderPageLatch(
                        "btree test leaf fill walk follows root pointers while retaining root")) {
                    leaf = ctx.access.openIndexPage(r, p.childPageId(), PageLatchMode.SHARED);
                }
                counts.add((long) leaf.header().nRecs());
            }
            ctx.mgr.commit(r);
            return counts;
        } catch (Throwable t) {
            ctx.mgr.rollbackUncommitted(r);
            throw t;
        }
    }

    /** 宽聚簇 KEY schema：column0 = varchar(5000) 主键、column1 = int payload；clustered=true。 */
    private static TableSchema wideKeySchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "k", ColumnType.varchar(5000, true), 0),
                new ColumnDef(new ColumnId(1), "v", ColumnType.intType(false, false), 1)), true);
    }

    private static IndexKeyDef wideKeyDef() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    /** id 顺序可排序的宽 key 值：%06d 前缀保证按 id 升序，再补到 5000B。 */
    private static String wideKeyValue(long id) {
        String prefix = String.format("%06d", id);
        return prefix + "x".repeat(5000 - prefix.length());
    }

    private static SearchKey kKey(long id) {
        return new SearchKey(List.of(new ColumnValue.StringValue(wideKeyValue(id))));
    }

    private static LogicalRecord wideKeyRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.StringValue(wideKeyValue(id)),
                new ColumnValue.IntValue(id)), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideKeyRowWithHidden(long id, TransactionId txnId, RollPointer rp) {
        return new LogicalRecord(1, List.of(new ColumnValue.StringValue(wideKeyValue(id)),
                new ColumnValue.IntValue(id)), false, RecordType.CONVENTIONAL, new HiddenColumns(txnId, rp));
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(5000, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("payload-" + id)), false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue("x".repeat(5000))), false, RecordType.CONVENTIONAL);
    }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            body.run(new Ctx(store, pool));
        }
    }

    private interface Body {
        void run(Ctx ctx);
    }

    private final class Ctx {
        private final MiniTransactionManager mgr = new MiniTransactionManager();
        private final DiskSpaceManager disk;
        private final IndexPageAccess access;
        private SegmentRef leafSegment;
        private SegmentRef nonLeafSegment;
        private PageId rootPageId;

        private Ctx(PageStore store, BufferPool pool) {
            this.disk = new DiskSpaceManager(pool, store, PS);
            this.access = new IndexPageAccess(pool, PS);
        }

        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        private void createTablespaceAndRoot() {
            MiniTransaction m = mgr.begin();
            disk.createTablespace(m, SPACE, dir.resolve("clustered.ibd"), PageNo.of(64));
            leafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(m, SPACE, SegmentPurpose.INDEX_NON_LEAF);
            rootPageId = disk.allocatePage(m, leafSegment);
            access.createIndexPage(m, rootPageId, INDEX_ID, 0);
            mgr.commit(m);
        }

        private BTreeIndex clusteredIndex() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, idKey(), clusteredSchema(), true,
                    leafSegment, nonLeafSegment);
        }
    }
}
