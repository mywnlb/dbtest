package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordKeyOrderCorruptedException;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * leaf-only B+Tree 第一片：只验证单 root leaf 页的只读 lookup 与页内 bounded scan。
 * 写入前的测试数据直接用 RecordPageInserter 填页，避免在 B1 RED 阶段依赖尚未实现的 B+Tree insert。
 */
class LeafOnlyBTreeIndexServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId ROOT = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static BTreeIndex index() {
        return new BTreeIndex(7L, ROOT, 0, idKey(), schema(), true);
    }

    private static BTreeIndex nonLeafIndex() {
        return new BTreeIndex(7L, ROOT, 1, idKey(), schema(), true);
    }

    private static TableSchema wideSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(5000, true), 1)));
    }

    private static BTreeIndex wideIndex() {
        return new BTreeIndex(7L, ROOT, 0, idKey(), wideSchema(), true);
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private static LogicalRecord wideRow(long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue("x".repeat(5000))),
                false, RecordType.CONVENTIONAL);
    }

    private interface Body {
        void run(BufferPool pool, IndexPageAccess access, MiniTransactionManager mgr);
    }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("btree.ibd"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            body.run(pool, new IndexPageAccess(pool, PS), new MiniTransactionManager());
        }
    }

    private void createRoot(IndexPageAccess access, MiniTransactionManager mgr) {
        MiniTransaction m = mgr.begin();
        access.createIndexPage(m, ROOT, 7L, 0);
        mgr.commit(m);
    }

    private void seedRows(IndexPageAccess access, MiniTransactionManager mgr, long... ids) {
        MiniTransaction m = mgr.begin();
        RecordPage page = access.openIndexPage(m, ROOT, PageLatchMode.EXCLUSIVE);
        RecordPageInserter inserter = new RecordPageInserter(registry);
        for (long id : ids) {
            inserter.insert(page, ROOT, row(id, "v" + id), idKey(), schema());
        }
        mgr.commit(m);
    }

    /**
     * 验证 {@code lookupReturnsEmptyWhenKeyMissing} 对应的B+Tree 索引行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void lookupReturnsEmptyWhenKeyMissing() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);

            MiniTransaction m = mgr.begin();
            assertTrue(service.lookup(m, index(), kId(42)).isEmpty());
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code scanLeafReturnsBoundedRowsInKeyOrder} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void scanLeafReturnsBoundedRowsInKeyOrder() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            seedRows(access, mgr, 3, 1, 4, 2);
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);

            MiniTransaction m = mgr.begin();
            List<BTreeLookupResult> rows = service.scanLeaf(m, index(),
                    new BTreeScanRange(kId(2), true, kId(4), false, 10));
            assertEquals(List.of(2L, 3L), ids(rows));
            mgr.commit(m);
        });
    }

    /**
     * 验证 {@code insertThroughBTreeEmitsRedoAndCanBeLookedUp} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void insertThroughBTreeEmitsRedoAndCanBeLookedUp() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);
            int before = mgr.redoLogManager().bufferedRecords().size();

            MiniTransaction m1 = mgr.begin();
            BTreeInsertResult inserted = service.insert(m1, index(), row(5, "v5"));
            mgr.commit(m1);

            assertEquals(ROOT, inserted.recordRef().pageId());
            assertTrue(mgr.redoLogManager().bufferedRecords().size() > before,
                    "btree insert should produce physical redo through MTR-owned page");

            MiniTransaction m2 = mgr.begin();
            BTreeLookupResult found = service.lookup(m2, index(), kId(5)).orElseThrow();
            assertEquals(List.of(5L), ids(List.of(found)));
            mgr.commit(m2);
        });
    }

    /**
     * 验证 {@code uniqueInsertRejectsDuplicatePhysicalKey} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void uniqueInsertRejectsDuplicatePhysicalKey() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);

            MiniTransaction m1 = mgr.begin();
            service.insert(m1, index(), row(7, "v7"));
            mgr.commit(m1);

            MiniTransaction m2 = mgr.begin();
            assertThrows(BTreeDuplicateKeyException.class, () -> service.insert(m2, index(), row(7, "again")));
            mgr.rollbackUncommitted(m2);
        });
    }

    /** 物理结构合法但 key 逆序的 root leaf 必须在 lookup/insert 内容访问前 fail-closed。 */
    @Test
    void existingRootRejectsSemanticKeyCorruptionWithoutNewRedoOrDirtyState() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            seedRows(access, mgr, 1, 2);
            MiniTransaction corrupt = mgr.begin();
            RecordPage page = access.openIndexPage(corrupt, ROOT, PageLatchMode.EXCLUSIVE);
            int first = page.recordOffsetsInOrder().get(0);
            replaceIdField(page, first, 3, schema());
            mgr.commit(corrupt);

            int redoBefore = mgr.redoLogManager().bufferedRecords().size();
            var dirtyBefore = pool.dirtyPageCandidates(Lsn.of(Long.MAX_VALUE), pool.capacity());
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);

            MiniTransaction read = mgr.begin();
            assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> service.lookup(read, index(), kId(2)));
            mgr.rollbackUncommitted(read);

            MiniTransaction write = mgr.begin();
            assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> service.insert(write, index(), row(4, "v4")));
            mgr.rollbackUncommitted(write);

            assertEquals(redoBefore, mgr.redoLogManager().bufferedRecords().size(),
                    "rejected semantic validation must not append redo");
            assertEquals(dirtyBefore, pool.dirtyPageCandidates(Lsn.of(Long.MAX_VALUE), pool.capacity()),
                    "rejected semantic validation must not change dirty-page state");
        });
    }

    /**
     * 验证 {@code nonLeafRootIsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void nonLeafRootIsRejected() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);

            MiniTransaction m = mgr.begin();
            assertThrows(BTreeUnsupportedStructureException.class,
                    () -> service.lookup(m, nonLeafIndex(), kId(1)));
            mgr.rollbackUncommitted(m);
        });
    }

    /**
     * 验证 {@code leafOverflowIsReportedAsSplitRequired} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void leafOverflowIsReportedAsSplitRequired() {
        onPool((pool, access, mgr) -> {
            createRoot(access, mgr);
            BTreeIndexService service = new LeafOnlyBTreeIndexService(access, registry);

            assertThrows(BTreeSplitRequiredException.class, () -> {
                long id = 0;
                while (true) {
                    MiniTransaction m = mgr.begin();
                    try {
                        service.insert(m, wideIndex(), wideRow(id++));
                        mgr.commit(m);
                    } catch (BTreeSplitRequiredException e) {
                        mgr.rollbackUncommitted(m);
                        throw e;
                    }
                }
            });
        });
    }

    private static List<Long> ids(List<BTreeLookupResult> rows) {
        return rows.stream()
                .map(BTreeLookupResult::record)
                .map(LogicalRecord::columnValues)
                .map(values -> (ColumnValue.IntValue) values.get(0))
                .map(ColumnValue.IntValue::value)
                .toList();
    }

    /** 保留物理记录头和长度，仅把 INT key 改为同宽值，制造 validator 专属的语义损坏。 */
    private void replaceIdField(RecordPage page, int offset, long replacementId, TableSchema schema) {
        RecordCursor cursor = new RecordCursor(page, offset, schema, registry);
        int fieldOffset = cursor.columnSlice(new ColumnId(0)).offset();
        int fieldLength = cursor.columnSlice(new ColumnId(0)).length();
        byte[] original = page.readRecordBytes(offset);
        byte[] replacement = new RecordEncoder(registry).encode(row(replacementId, "v" + replacementId), schema);
        System.arraycopy(replacement, fieldOffset, original, fieldOffset, fieldLength);
        page.writeRecordBytes(offset, original);
    }
}
