package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证紧凑二级索引 entry 的专用 B+Tree 原语。测试坚持使用完整物理 key（逻辑 key + 聚簇主键后缀），
 * 从而同时覆盖“逻辑值可重复、物理 identity 不可重复”以及 delete-marked entry 仍占用物理 identity 的语义。
 */
class BTreeSecondaryIndexOperationTest {

    /** 测试页大小；使用标准 16 KiB，避免测试布局偏离生产配置。 */
    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    /** 独立测试表空间，隔离其它 B+Tree 测试的页号与 segment identity。 */
    private static final SpaceId SPACE_ID = SpaceId.of(74);
    /** 二级索引 id，同时写入 root page header 与运行时 descriptor。 */
    private static final long INDEX_ID = 404L;

    /** JUnit 管理的临时目录；每个测试结束后自动清理物理表空间文件。 */
    @TempDir
    Path directory;

    /** 类型 codec 注册表；页内编码、比较与测试物化共享同一稳定定义。 */
    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    /**
     * 二级索引的 logical unique 与 physical unique 必须分离：相同 email、不同主键可共存，
     * 但同一完整物理 key 的重复发布必须被 B+Tree 拒绝。
     */
    @Test
    void publishesDifferentPrimaryKeysButRejectsDuplicatePhysicalKey() {
        onTree(context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex index = context.index();

            MiniTransaction write = context.transactions.begin();
            service.insertSecondary(write, index, entry("a@example.test", 1));
            service.insertSecondary(write, index, entry("a@example.test", 2));
            assertThrows(BTreeDuplicateKeyException.class,
                    () -> service.insertSecondary(write, index, entry("a@example.test", 1)));
            context.transactions.commit(write);

            MiniTransaction read = context.transactions.begin();
            assertTrue(service.lookup(read, index, physicalKey("a@example.test", 1)).isPresent());
            assertTrue(service.lookup(read, index, physicalKey("a@example.test", 2)).isPresent());
            context.transactions.commit(read);
        });
    }

    /**
     * delete-mark 只改变二级记录头，不删除紧凑字段；普通 lookup 过滤该 entry，including-deleted lookup
     * 仍可取得完整主键后缀，并允许 rollback 通过 revive 恢复为 live。
     */
    @Test
    void marksAndRevivesSecondaryEntryWithoutLosingClusterKeySuffix() {
        onTree(context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex index = context.index();
            MiniTransaction insert = context.transactions.begin();
            service.insertSecondary(insert, index, entry("b@example.test", 7));
            context.transactions.commit(insert);

            MiniTransaction mark = context.transactions.begin();
            assertEquals(SecondaryDeleteMarkStatus.CHANGED,
                    service.setSecondaryDeleteMark(mark, index, physicalKey("b@example.test", 7), true).status());
            assertEquals(SecondaryDeleteMarkStatus.ALREADY_IN_STATE,
                    service.setSecondaryDeleteMark(mark, index, physicalKey("b@example.test", 7), true).status());
            context.transactions.commit(mark);

            MiniTransaction hiddenRead = context.transactions.begin();
            assertTrue(service.lookup(hiddenRead, index, physicalKey("b@example.test", 7)).isEmpty());
            BTreeLookupResult marked = service.lookupIncludingDeleted(hiddenRead, index,
                    physicalKey("b@example.test", 7)).orElseThrow();
            assertTrue(marked.record().deleted());
            assertEquals(new ColumnValue.IntValue(7), marked.record().columnValues().get(1));
            context.transactions.commit(hiddenRead);

            MiniTransaction revive = context.transactions.begin();
            assertEquals(SecondaryDeleteMarkStatus.CHANGED,
                    service.setSecondaryDeleteMark(revive, index, physicalKey("b@example.test", 7), false).status());
            context.transactions.commit(revive);

            MiniTransaction visibleRead = context.transactions.begin();
            assertFalse(service.lookup(visibleRead, index, physicalKey("b@example.test", 7))
                    .orElseThrow().record().deleted());
            context.transactions.commit(visibleRead);
        });
    }

    /**
     * purge 只能物理摘除已标记 entry：live 命中必须显式报告状态冲突且保持原页不变；成功后再次处理同一任务
     * 返回 ABSENT，使 crash recovery 或 history 重试能够幂等收敛。
     */
    @Test
    void purgeRejectsLiveEntryAndIsIdempotentAfterRemoval() {
        onTree(context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex index = context.index();
            SearchKey key = physicalKey("c@example.test", 9);
            MiniTransaction insert = context.transactions.begin();
            service.insertSecondary(insert, index, entry("c@example.test", 9));
            context.transactions.commit(insert);

            MiniTransaction livePurge = context.transactions.begin();
            assertEquals(SecondaryEntryRemovalStatus.STATE_CONFLICT,
                    service.purgeDeleteMarkedSecondary(livePurge, index, key).status());
            context.transactions.commit(livePurge);

            MiniTransaction mark = context.transactions.begin();
            service.setSecondaryDeleteMark(mark, index, key, true);
            context.transactions.commit(mark);

            MiniTransaction purge = context.transactions.begin();
            BTreeSecondaryRemovalResult removed = service.purgeDeleteMarkedSecondary(purge, index, key);
            context.transactions.commit(purge);
            assertEquals(SecondaryEntryRemovalStatus.REMOVED, removed.status());

            MiniTransaction retry = context.transactions.begin();
            assertEquals(SecondaryEntryRemovalStatus.ABSENT,
                    service.purgeDeleteMarkedSecondary(retry, removed.indexAfter(), key).status());
            context.transactions.commit(retry);
        });
    }

    /**
     * INSERT/UPDATE rollback 对刚发布的 live entry 使用物理删除；缺失 entry 是已完成证明，
     * delete-marked entry 则属于错误 inverse，必须报告状态冲突而不能静默回收。
     */
    @Test
    void rollbackPhysicalDeleteRequiresLiveEntry() {
        onTree(context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex index = context.index();
            SearchKey first = physicalKey("d@example.test", 11);
            SearchKey second = physicalKey("e@example.test", 12);
            MiniTransaction insert = context.transactions.begin();
            service.insertSecondary(insert, index, entry("d@example.test", 11));
            service.insertSecondary(insert, index, entry("e@example.test", 12));
            service.setSecondaryDeleteMark(insert, index, second, true);
            context.transactions.commit(insert);

            MiniTransaction rollback = context.transactions.begin();
            BTreeSecondaryRemovalResult removed = service.deletePublishedSecondary(rollback, index, first);
            assertEquals(SecondaryEntryRemovalStatus.REMOVED, removed.status());
            assertEquals(SecondaryEntryRemovalStatus.ABSENT,
                    service.deletePublishedSecondary(rollback, removed.indexAfter(), first).status());
            assertEquals(SecondaryEntryRemovalStatus.STATE_CONFLICT,
                    service.deletePublishedSecondary(rollback, removed.indexAfter(), second).status());
            context.transactions.commit(rollback);
        });
    }

    /**
     * including-deleted scan 必须保留物理 key 顺序并返回标记候选；普通 scan 仍过滤标记项，
     * 以免唯一检查、purge 的物理视图污染普通查询语义。
     */
    @Test
    void scanIncludingDeletedDoesNotChangeNormalScanFiltering() {
        onTree(context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex index = context.index();
            MiniTransaction write = context.transactions.begin();
            service.insertSecondary(write, index, entry("f@example.test", 1));
            service.insertSecondary(write, index, entry("f@example.test", 2));
            service.insertSecondary(write, index, entry("f@example.test", 3));
            service.setSecondaryDeleteMark(write, index, physicalKey("f@example.test", 2), true);
            context.transactions.commit(write);

            BTreeScanRange range = new BTreeScanRange(physicalKey("f@example.test", 1), true,
                    physicalKey("f@example.test", 3), true, 10);
            MiniTransaction read = context.transactions.begin();
            assertEquals(List.of(1L, 3L), ids(service.scan(read, index, range)));
            assertEquals(List.of(1L, 2L, 3L), ids(service.scanIncludingDeleted(read, index, range)));
            context.transactions.commit(read);
        });
    }

    /**
     * DD binding 中的 root level 只是打开时提示；前一次独立 MTR 触发 root grow 后，后续结构操作必须从
     * root page header 刷新快照，不能继续依赖陈旧 level-0 descriptor 冻结 redo/空间预算。
     */
    @Test
    void refreshesRootLevelFromPageHeaderAfterIndependentSplit() {
        onTree(context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex stale = context.index();
            BTreeIndex current = stale;
            for (int id = 1; id <= 140 && current.rootLevel() == 0; id++) {
                String email = String.format("%03d%s", id, "x".repeat(120));
                MiniTransaction write = context.transactions.begin();
                current = service.insertSecondary(write, current, entry(email, id)).indexAfterInsert();
                context.transactions.commit(write);
            }
            assertTrue(current.rootLevel() > stale.rootLevel(), "测试前置条件：独立写 MTR 必须已触发 root grow");

            MiniTransaction refresh = context.transactions.begin();
            BTreeIndex refreshed = new BTreeRootSnapshotService(context.access).refresh(refresh, stale);
            context.transactions.commit(refresh);

            assertEquals(current.rootLevel(), refreshed.rootLevel());
            assertEquals(stale.rootPageId(), refreshed.rootPageId(), "root grow 不改变稳定 root page identity");
        });
    }

    /**
     * secondary 物理删除必须复用聚簇树相同的 merge/root-shrink 算法；小 Buffer Pool 下每条操作均使用独立短 MTR，
     * 不能依赖把整棵多层树常驻内存，也不能在 root 降级后继续使用陈旧 level 快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用宽 secondary key 连续插入并逐 MTR 提交，直到 root page header 证明树高达到两层以上。</li>
     *     <li>逐 entry 先提交 delete-mark，再在下一 MTR 物理 purge，并消费每次返回的最新 index 快照。</li>
     *     <li>断言所有 entry 删除后 root 原地收缩为 level 0、至少释放一个结构页，且旧 key 全部不可见。</li>
     *     <li>在同一稳定 root page 上重新插入并读取，证明 merge/root-shrink 后树仍可继续使用。</li>
     * </ol>
     */
    @Test
    void multiLevelSecondaryTreeShrinksUnderSmallBufferPool() {
        onTree(24, context -> {
            SplitCapableBTreeIndexService service = context.service();
            BTreeIndex current = context.index();
            List<String> emails = new java.util.ArrayList<>();

            // 1. 宽 key 压低 leaf/internal fan-out；每次提交后允许 Buffer Pool 淘汰旧路径页。
            for (int id = 1; id <= 48 && current.rootLevel() < 2; id++) {
                String email = String.format("%03d-%s", id, "x".repeat(3_500));
                MiniTransaction insert = context.transactions.begin();
                current = service.insertSecondary(insert, current, entry(email, id)).indexAfterInsert();
                context.transactions.commit(insert);
                emails.add(email);
            }
            assertTrue(current.rootLevel() >= 2, "测试必须先构造多层 secondary tree");

            // 2. mark 与 purge 分属独立 MTR；purge 返回值是 root shrink 后续操作的唯一结构快照。
            int freedPages = 0;
            for (int offset = 0; offset < emails.size(); offset++) {
                long id = offset + 1L;
                SearchKey key = physicalKey(emails.get(offset), id);
                MiniTransaction mark = context.transactions.begin();
                assertEquals(SecondaryDeleteMarkStatus.CHANGED,
                        service.setSecondaryDeleteMark(mark, current, key, true).status());
                context.transactions.commit(mark);

                MiniTransaction purge = context.transactions.begin();
                BTreeSecondaryRemovalResult result = service.purgeDeleteMarkedSecondary(purge, current, key);
                context.transactions.commit(purge);
                assertEquals(SecondaryEntryRemovalStatus.REMOVED, result.status());
                current = result.indexAfter();
                freedPages += result.freedPages().size();
            }

            // 3. root identity 稳定，但页头 level 必须随级联 merge 收缩到单叶；回收结果证明走过结构删除分支。
            assertEquals(0, current.rootLevel());
            assertEquals(context.rootPageId, current.rootPageId());
            assertTrue(freedPages > 0, "secondary merge/root shrink must return reclaimed pages");
            MiniTransaction emptyRead = context.transactions.begin();
            for (int offset = 0; offset < emails.size(); offset++) {
                assertTrue(service.lookupIncludingDeleted(emptyRead, current,
                        physicalKey(emails.get(offset), offset + 1L)).isEmpty());
            }
            context.transactions.commit(emptyRead);

            // 4. 收缩后的稳定 root 仍可接收新 entry，避免只验证空树表象而遗漏页头/segment 损坏。
            MiniTransaction reuse = context.transactions.begin();
            current = service.insertSecondary(reuse, current, entry("reused@example.test", 999)).indexAfterInsert();
            context.transactions.commit(reuse);
            MiniTransaction verify = context.transactions.begin();
            assertTrue(service.lookup(verify, current,
                    physicalKey("reused@example.test", 999)).isPresent());
            context.transactions.commit(verify);
        });
    }

    /** 从紧凑 entry 的第二列提取完整聚簇主键，便于断言扫描顺序和 marked 候选是否保留。 */
    private static List<Long> ids(List<BTreeLookupResult> rows) {
        return rows.stream()
                .map(row -> ((ColumnValue.IntValue) row.record().columnValues().get(1)).value())
                .toList();
    }

    /** 构造物理二级 key；第一部分是 logical email，第二部分是完整聚簇主键后缀。 */
    private static SearchKey physicalKey(String email, long id) {
        return new SearchKey(List.of(new ColumnValue.StringValue(email), new ColumnValue.IntValue(id)));
    }

    /** 构造不携带聚簇隐藏列的紧凑二级记录。 */
    private static LogicalRecord entry(String email, long id) {
        return new LogicalRecord(1, List.of(new ColumnValue.StringValue(email), new ColumnValue.IntValue(id)),
                false, RecordType.CONVENTIONAL);
    }

    /** 定义紧凑二级 entry schema；ordinal 只对应 entry 内字段，不复用原表 ordinal。 */
    private static TableSchema secondarySchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "email", ColumnType.varchar(4096, false), 0),
                new ColumnDef(new ColumnId(1), "id", ColumnType.intType(false, false), 1)), false);
    }

    /** 二级树使用完整物理 key，保证同一逻辑值下不同主键 entry 的确定排序。 */
    private static IndexKeyDef physicalKeyDefinition() {
        return new IndexKeyDef(INDEX_ID, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
    }

    /** 在真实临时 tablespace、segment、Buffer Pool 上执行测试，确保页结构与 MTR 生命周期均走生产实现。 */
    private void onTree(TestBody body) {
        onTree(64, body);
    }

    /** 使用指定 Buffer Pool 容量运行真实二级树测试，供淘汰/短 MTR 边界压力场景复用。 */
    private void onTree(int capacity, TestBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PAGE_SIZE, capacity)) {
            TestContext context = new TestContext(store, pool);
            context.create();
            body.run(context);
        }
    }

    /** 测试主体函数；不向测试暴露页句柄，避免绕过 B+Tree API 直接制造状态。 */
    private interface TestBody {
        void run(TestContext context);
    }

    /** 真实二级树测试装配，持有 MTR、磁盘与 index page access 的明确生命周期。 */
    private final class TestContext {
        /** 所有测试操作的 MTR owner；每个逻辑步骤显式 begin/commit。 */
        private final MiniTransactionManager transactions = new MiniTransactionManager();
        /** 表空间与 segment/page 分配入口。 */
        private final DiskSpaceManager disk;
        /** B+Tree 页创建、fix 与 latch 的稳定访问入口。 */
        private final IndexPageAccess access;
        /** 二级 leaf segment；split 时由 B+Tree 使用。 */
        private SegmentRef leafSegment;
        /** 二级 non-leaf segment；树长高时由 B+Tree 使用。 */
        private SegmentRef nonLeafSegment;
        /** 稳定 root page；root split/shrink 只改变页内 level，不改变该 identity。 */
        private PageId rootPageId;

        private TestContext(PageStore store, BufferPool pool) {
            this.disk = new DiskSpaceManager(pool, store, PAGE_SIZE);
            this.access = new IndexPageAccess(pool, PAGE_SIZE);
        }

        /**
         * 创建 tablespace、两个 index segment 与 level-0 root。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>在单个初始化 MTR 中创建临时表空间，保证 page-0 元数据先于 segment 分配存在。</li>
         *     <li>分别创建 leaf/non-leaf segment，维持后续 split 的页面归属。</li>
         *     <li>从 leaf segment 分配 root 并写入 index-id/level header，提交后才允许业务 MTR 访问。</li>
         * </ol>
         */
        private void create() {
            // 1. 表空间初始化与后续 segment 元数据写入属于同一个短 MTR。
            MiniTransaction initialization = transactions.begin();
            disk.createTablespace(initialization, SPACE_ID, directory.resolve("secondary.ibd"), PageNo.of(64));

            // 2. 分开建立 leaf/non-leaf segment，避免把不同职责页面混入同一分配域。
            leafSegment = disk.createSegment(initialization, SPACE_ID, SegmentPurpose.INDEX_LEAF);
            nonLeafSegment = disk.createSegment(initialization, SPACE_ID, SegmentPurpose.INDEX_NON_LEAF);

            // 3. root 从 leaf segment 分配并格式化为 level-0；commit 后页面 redo/dirty 状态才对测试业务可见。
            rootPageId = disk.allocatePage(initialization, leafSegment);
            access.createIndexPage(initialization, rootPageId, INDEX_ID, 0);
            transactions.commit(initialization);
        }

        /** 创建无共享可变状态的 B+Tree service；底层 access/disk 生命周期归当前测试 context。 */
        private SplitCapableBTreeIndexService service() {
            return new SplitCapableBTreeIndexService(access, disk, registry);
        }

        /** 返回二级索引初始快照；完整物理 key 始终唯一，logical unique 不在该描述符中表达。 */
        private BTreeIndex index() {
            return new BTreeIndex(INDEX_ID, rootPageId, 0, physicalKeyDefinition(), secondarySchema(), true,
                    leafSegment, nonLeafSegment);
        }
    }
}
