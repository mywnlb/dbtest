package cn.zhangyis.db.storage.api.index;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.CachingTablespaceRegistry;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.PageDirectoryCorruptedException;
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
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** IndexPageAccess：createIndexPage 产 PAGE_INIT(INDEX)+格式 redo、盖 pageLSN；openIndexPage 供算子 CRUD/只读；参数前置校验。 */
class IndexPageAccessTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));

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

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface Body { void run(BufferPool pool, IndexPageAccess access, MiniTransactionManager mgr); }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            body.run(pool, new IndexPageAccess(pool, PS), new MiniTransactionManager());
        }
    }

    @Test
    void createIndexPageEmitsInitAndFormatRedo() {
        onPool((pool, access, mgr) -> {
            MiniTransaction m1 = mgr.begin();
            access.createIndexPage(m1, P, 7L, 0);
            mgr.commit(m1);

            List<?> recs = mgr.redoLogManager().bufferedRecords();
            assertTrue(recs.stream().anyMatch(r -> r instanceof PageInitRecord pir
                    && pir.pageId().equals(P) && pir.pageType() == PageType.INDEX), "PAGE_INIT(INDEX)");
            assertTrue(recs.stream().anyMatch(r -> r instanceof PageBytesRecord), "format PAGE_BYTES");
            Lsn endLsn = mgr.redoLogManager().currentLsn();

            // 读验证 MTR：断言后显式 commit 释放 guard。
            MiniTransaction m2 = mgr.begin();
            RecordPage rp2 = access.openIndexPage(m2, P, PageLatchMode.SHARED);
            assertEquals(7L, rp2.header().indexId());
            assertEquals(0, rp2.header().level());
            PageGuard g = m2.getPage(pool, P, PageLatchMode.SHARED); // 同 MTR 重入 S，读信封
            assertEquals(PageType.INDEX, PageEnvelope.readHeader(g).pageType());
            assertEquals(endLsn, PageEnvelope.readPageLsn(g));
            mgr.commit(m2);
        });
    }

    @Test
    void insertThroughMtrOwnedPageEmitsRedo() {
        onPool((pool, access, mgr) -> {
            TableSchema schema = schema();
            IndexKeyDef kd = idKey();
            // m1：仅 createIndexPage（format redo），记下 redo 条数基线。
            MiniTransaction m1 = mgr.begin();
            access.createIndexPage(m1, P, 7L, 0);
            mgr.commit(m1);
            int afterFormat = mgr.redoLogManager().bufferedRecords().size();

            // m2：openIndexPage(X) + insert → 经 MTR-owned guard 写记录，redo 应增长。
            MiniTransaction m2 = mgr.begin();
            RecordPage rp = access.openIndexPage(m2, P, PageLatchMode.EXCLUSIVE);
            new RecordPageInserter(registry).insert(rp, P, row(1, "a"), kd, schema);
            mgr.commit(m2);
            assertTrue(mgr.redoLogManager().bufferedRecords().size() > afterFormat,
                    "insert via MTR-owned page produced redo");

            // m3：重开查回（读验证 MTR 显式 commit）。
            MiniTransaction m3 = mgr.begin();
            RecordPage rp3 = access.openIndexPage(m3, P, PageLatchMode.SHARED);
            assertTrue(new RecordPageSearch(registry).findEqual(rp3, kId(1), kd, schema).isPresent(), "id=1 found");
            mgr.commit(m3);
        });
    }

    @Test
    void openSharedProducesNoRedo() {
        onPool((pool, access, mgr) -> {
            MiniTransaction m1 = mgr.begin();
            access.createIndexPage(m1, P, 7L, 0);
            mgr.commit(m1);
            int before = mgr.redoLogManager().bufferedRecords().size();
            MiniTransaction mr = mgr.begin();
            PageGuard g0 = mr.getPage(pool, P, PageLatchMode.SHARED);
            Lsn pageLsnBefore = PageEnvelope.readPageLsn(g0);
            mgr.commit(mr);

            // 只读 MTR：openIndexPage(S) 读 header，不写。
            MiniTransaction m2 = mgr.begin();
            RecordPage rp2 = access.openIndexPage(m2, P, PageLatchMode.SHARED);
            rp2.header(); // 只读
            mgr.commit(m2);

            assertEquals(before, mgr.redoLogManager().bufferedRecords().size(), "S-only adds no redo");
            MiniTransaction m3 = mgr.begin();
            PageGuard g = m3.getPage(pool, P, PageLatchMode.SHARED);
            assertEquals(pageLsnBefore, PageEnvelope.readPageLsn(g), "S-only does not restamp pageLSN");
            mgr.commit(m3);
        });
    }

    /** 已格式化页的结构损坏必须在统一 open 边界被 S/X 两种访问拒绝，不能继续进入 B+Tree 解析或修改。 */
    @Test
    void openRejectsCorruptedRecordStructureInSharedAndExclusiveModes() {
        onPool((pool, access, mgr) -> {
            MiniTransaction create = mgr.begin();
            access.createIndexPage(create, P, 7L, 0);
            mgr.commit(create);

            MiniTransaction corrupt = mgr.begin();
            RecordPage page = access.openIndexPage(corrupt, P, PageLatchMode.EXCLUSIVE);
            page.writeHeader(page.header().withNRecs(1));
            mgr.commit(corrupt);
            int redoBeforeRejectedOpens = mgr.redoLogManager().bufferedRecords().size();

            MiniTransaction shared = mgr.begin();
            assertThrows(PageDirectoryCorruptedException.class,
                    () -> access.openIndexPage(shared, P, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(shared);

            MiniTransaction exclusive = mgr.begin();
            assertThrows(PageDirectoryCorruptedException.class,
                    () -> access.openIndexPage(exclusive, P, PageLatchMode.EXCLUSIVE));
            mgr.rollbackUncommitted(exclusive);
            assertEquals(redoBeforeRejectedOpens, mgr.redoLogManager().bufferedRecords().size(),
                    "structure validation is read-only even for an EXCLUSIVE open");
        });
    }

    @Test
    void createIndexPageValidatesArgsBeforeTouchingPage() {
        onPool((pool, access, mgr) -> {
            MiniTransaction m = mgr.begin();
            assertThrows(DatabaseValidationException.class, () -> access.createIndexPage(m, P, -1L, 0));
            assertThrows(DatabaseValidationException.class, () -> access.createIndexPage(m, P, 7L, -1));
            assertThrows(DatabaseValidationException.class, () -> access.createIndexPage(m, null, 7L, 0));
            assertThrows(DatabaseValidationException.class, () -> access.openIndexPage(m, P, null));
            // 非法入参在 newPage 前抛 → 页未被改、无 redo。
            assertTrue(mgr.redoLogManager().bufferedRecords().isEmpty(), "no redo produced on validation failure");
            mgr.rollbackUncommitted(m);
        });
    }

    /**
     * registry-aware INDEX access 必须在 MTR 取得表空间 S lease 后复核运行时状态。INACTIVE 时不能继续进入
     * Buffer Pool，也不能执行破坏性的 newPage 重初始化，否则绕过 DiskSpaceManager 的调用方会访问截断/下线空间。
     */
    @Test
    void registryAwareIndexAccessRejectsInactiveTablespaceBeforeTouchingPage() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("inactive-index.ibd"), PS, PageNo.of(8));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            TablespaceRegistry tablespaces = registryFor(TablespaceState.NORMAL);
            tablespaces.markInactive(SPACE);
            IndexPageAccess access = new IndexPageAccess(pool, PS, tablespaces);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction m = mgr.begin();

            assertThrows(TablespaceUnavailableException.class, () -> access.createIndexPage(m, P, 7L, 0));
            assertThrows(TablespaceUnavailableException.class,
                    () -> access.openIndexPage(m, P, PageLatchMode.SHARED));
            assertTrue(mgr.redoLogManager().bufferedRecords().isEmpty(), "inactive access must not touch the page");
            mgr.rollbackUncommitted(m);
        }
    }

    private TablespaceRegistry registryFor(TablespaceState state) {
        TablespaceMetadata metadata = new TablespaceMetadata(SPACE, "space-" + SPACE.value(),
                TablespaceType.GENERAL, PS, state,
                List.of(DataFileDescriptor.single(dir.resolve("registry.ibd"), PageNo.of(0), PageNo.of(8))),
                new SpaceFlags(TablespaceTypeFlags.encode(TablespaceType.GENERAL)), PageNo.of(8), PageNo.of(0), 1L);
        return new CachingTablespaceRegistry(spaceId -> SPACE.equals(spaceId)
                ? java.util.Optional.of(metadata) : java.util.Optional.empty());
    }
}
