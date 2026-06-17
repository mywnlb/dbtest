package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.IndexPageAccess;
import cn.zhangyis.db.storage.api.IndexPageHandle;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.page.IndexPageHeader;
import cn.zhangyis.db.storage.record.page.RecordComparator;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPageOverflowException;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.RecordRef;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * B3 split-capable B+Tree 实现。范围严格限制为 root leaf split、level-1 root-to-leaf 路由、
 * leaf sibling range scan，以及 level-1 leaf split；root 高度超过 1 或 parent split 均显式拒绝。
 *
 * <p>锁顺序：root → 目标 leaf → 分配新 leaf → 原右邻居。当前没有事务锁/MVCC，所有页 latch 均由调用方 MTR memo
 * 统一持有并在 commit/rollback 释放。页内容修改全部通过 MTR-owned {@link IndexPageAccess} 或 {@link IndexPageHandle}
 * 完成，依赖 D3/D4 的物理 PAGE_BYTES/PAGE_INIT redo 与 commit pageLSN。
 */
public final class SplitCapableBTreeIndexService implements BTreeIndexService {

    /** INDEX 页访问 facade；不暴露 BufferFrame，只返回 MTR-owned 页视图。 */
    private final IndexPageAccess pageAccess;
    /** 空间管理 facade；split 分配新 leaf 页必须走 segment/page 分配路径。 */
    private final DiskSpaceManager disk;
    /** 类型 codec 注册表；record 比较、编码、物化共享同一套类型规则。 */
    private final TypeCodecRegistry registry;
    /** 页内查找器；root node pointer 选择与 leaf duplicate 检查复用其目录二分逻辑。 */
    private final RecordPageSearch search;
    /** 页内插入器；负责 key 有序链入与 PageDirectory 维护。 */
    private final RecordPageInserter inserter;
    /** leaf record 与 scan 边界的比较器。 */
    private final RecordComparator recordComparator;
    /** 内存中 split 列表排序用比较器，复用类型 codec 保序编码。 */
    private final SearchKeyComparator keyComparator;
    /** node pointer 逻辑模型与 LogicalRecord 的转换器。 */
    private final BTreeNodePointerCodec pointerCodec;
    /** 预估父页是否容纳新 node pointer 时使用的编码器。 */
    private final RecordEncoder recordEncoder;

    public SplitCapableBTreeIndexService(IndexPageAccess pageAccess, DiskSpaceManager disk,
                                         TypeCodecRegistry registry) {
        if (pageAccess == null || disk == null || registry == null) {
            throw new DatabaseValidationException("split btree pageAccess/disk/registry must not be null");
        }
        this.pageAccess = pageAccess;
        this.disk = disk;
        this.registry = registry;
        this.search = new RecordPageSearch(registry);
        this.inserter = new RecordPageInserter(registry);
        this.recordComparator = new RecordComparator(registry);
        this.keyComparator = new SearchKeyComparator(registry);
        this.pointerCodec = new BTreeNodePointerCodec();
        this.recordEncoder = new RecordEncoder(registry);
    }

    /**
     * 点查索引。数据流：打开 root 并校验 header → level=0 直接查 leaf；
     * level=1 用 root node pointer 选择 leaf，再在 leaf 内执行页内等值查找。
     */
    @Override
    public Optional<BTreeLookupResult> lookup(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        if (key == null) {
            throw new DatabaseValidationException("btree lookup key must not be null");
        }
        IndexPageHandle rootHandle = openRoot(mtr, index, PageLatchMode.SHARED);
        RecordPage root = rootHandle.recordPage();
        if (index.rootLevel() == 0) {
            return lookupInLeaf(root, index.rootPageId(), index, key);
        }
        if (index.rootLevel() == 1) {
            PageId child = chooseChild(root, index, key);
            RecordPage leaf = pageAccess.openIndexPage(mtr, child, PageLatchMode.SHARED);
            validateLeafPage(leaf, index, child);
            return lookupInLeaf(leaf, child, index, key);
        }
        throw new BTreeUnsupportedStructureException("split btree supports rootLevel 0 or 1 only: "
                + index.rootLevel());
    }

    /**
     * 历史 scanLeaf 方法在 B3 后委托为真正 scan。leaf-only 语义仍由 LeafOnlyBTreeIndexService 保持。
     */
    @Override
    public List<BTreeLookupResult> scanLeaf(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        return scan(mtr, index, range);
    }

    /**
     * 有界范围扫描。level=0 扫 root leaf；level=1 从 lowerKey 对应 leaf 开始，沿 FIL sibling next 链顺序扫。
     */
    @Override
    public List<BTreeLookupResult> scan(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        if (range == null) {
            throw new DatabaseValidationException("btree scan range must not be null");
        }
        IndexPageHandle rootHandle = openRoot(mtr, index, PageLatchMode.SHARED);
        if (range.limit() == 0) {
            return List.of();
        }
        if (index.rootLevel() == 0) {
            ScanAccumulator acc = new ScanAccumulator(range.limit());
            scanLeafPage(rootHandle.recordPage(), index.rootPageId(), index, range, acc);
            return acc.results();
        }
        if (index.rootLevel() == 1) {
            PageId leafId = chooseChild(rootHandle.recordPage(), index, range.lowerKey());
            ScanAccumulator acc = new ScanAccumulator(range.limit());
            while (true) {
                IndexPageHandle leafHandle = pageAccess.openIndexPageHandle(mtr, leafId, PageLatchMode.SHARED);
                RecordPage leaf = leafHandle.recordPage();
                validateLeafPage(leaf, index, leafId);
                boolean stop = scanLeafPage(leaf, leafId, index, range, acc);
                if (stop || acc.full()) {
                    break;
                }
                long next = leafHandle.fileHeader().nextPageNo();
                if (next == FilePageHeader.FIL_NULL) {
                    break;
                }
                leafId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
            }
            return acc.results();
        }
        throw new BTreeUnsupportedStructureException("split btree supports rootLevel 0 or 1 only: "
                + index.rootLevel());
    }

    /**
     * 插入逻辑记录。level=0 先尝试 root leaf 直接插入，页满则执行 root split；
     * level=1 先路由到目标 leaf，页满则执行 leaf split 并向 root 插入新 node pointer。
     */
    @Override
    public BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("btree insert record must not be null");
        }
        SearchKey key = keyOf(record, index);
        IndexPageHandle rootHandle = openRoot(mtr, index, PageLatchMode.EXCLUSIVE);
        RecordPage root = rootHandle.recordPage();
        if (index.rootLevel() == 0) {
            ensureUniqueAbsent(root, index.rootPageId(), index, key);
            try {
                RecordRef ref = inserter.insert(root, index.rootPageId(), record, index.keyDef(), index.schema());
                return new BTreeInsertResult(index, ref);
            } catch (RecordPageOverflowException overflow) {
                requireLeafSegment(index);
                return splitRootLeaf(mtr, index, rootHandle, record);
            }
        }
        if (index.rootLevel() == 1) {
            PageId leafId = chooseChild(root, index, key);
            IndexPageHandle leafHandle = pageAccess.openIndexPageHandle(mtr, leafId, PageLatchMode.EXCLUSIVE);
            RecordPage leaf = leafHandle.recordPage();
            validateLeafPage(leaf, index, leafId);
            ensureUniqueAbsent(leaf, leafId, index, key);
            try {
                RecordRef ref = inserter.insert(leaf, leafId, record, index.keyDef(), index.schema());
                return new BTreeInsertResult(index, ref);
            } catch (RecordPageOverflowException overflow) {
                requireLeafSegment(index);
                return splitLevelOneLeaf(mtr, index, rootHandle, leafHandle, record);
            }
        }
        throw new BTreeUnsupportedStructureException("split btree supports rootLevel 0 or 1 only: "
                + index.rootLevel());
    }

    /** root leaf 页满时的稳定 root split：旧 root 内容物化到内存，root 重建为 level-1，两个新 leaf 保存旧/新记录。 */
    private BTreeInsertResult splitRootLeaf(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle,
                                           LogicalRecord inserted) {
        RecordPage oldRootLeaf = rootHandle.recordPage();
        List<LogicalRecord> records = sortedWithInserted(materializeLeafRecords(oldRootLeaf, index), inserted, index);
        SplitRows split = splitRows(records);

        PageId leftId = disk.allocatePage(mtr, index.leafSegment());
        RecordPage leftPage = pageAccess.createIndexPage(mtr, leftId, index.indexId(), 0);
        PageId rightId = disk.allocatePage(mtr, index.leafSegment());
        RecordPage rightPage = pageAccess.createIndexPage(mtr, rightId, index.indexId(), 0);
        IndexPageHandle leftHandle = pageAccess.openIndexPageHandle(mtr, leftId, PageLatchMode.EXCLUSIVE);
        IndexPageHandle rightHandle = pageAccess.openIndexPageHandle(mtr, rightId, PageLatchMode.EXCLUSIVE);
        leftHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, rightId.pageNo().value());
        rightHandle.writeSiblingLinks(leftId.pageNo().value(), FilePageHeader.FIL_NULL);

        RecordRef insertedRef = insertAll(leftPage, leftId, split.left(), index, inserted);
        RecordRef rightInsertedRef = insertAll(rightPage, rightId, split.right(), index, inserted);
        if (insertedRef == null) {
            insertedRef = rightInsertedRef;
        }

        RecordPage root = rootHandle.recordPage();
        root.format(index.indexId(), 1);
        rootHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
        BTreeIndex after = index.withRootLevel(1);
        insertRootPointer(root, after, new BTreeNodePointer(lowKey(split.left(), index), leftId));
        insertRootPointer(root, after, new BTreeNodePointer(lowKey(split.right(), index), rightId));
        return new BTreeInsertResult(index, insertedRef, after, true, List.of(leftId, rightId));
    }

    /**
     * level-1 leaf split：先确认 root 能容纳一个新 pointer，再重写旧 leaf 并创建一个新 right leaf。
     * 当前切片不实现 parent split，因此父页不足时必须在任何 leaf rewrite 前失败。
     */
    private BTreeInsertResult splitLevelOneLeaf(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle,
                                               IndexPageHandle oldLeafHandle, LogicalRecord inserted) {
        RecordPage root = rootHandle.recordPage();
        RecordPage oldLeaf = oldLeafHandle.recordPage();
        PageId oldLeafId = oldLeafHandle.pageId();
        FilePageHeader oldLeafHeader = oldLeafHandle.fileHeader();
        List<LogicalRecord> records = sortedWithInserted(materializeLeafRecords(oldLeaf, index), inserted, index);
        SplitRows split = splitRows(records);
        BTreeNodePointer newPointer = new BTreeNodePointer(lowKey(split.right(), index),
                PageId.of(oldLeafId.spaceId(), PageNo.of(0)));
        ensureRootHasRoomForPointer(root, index, newPointer);

        PageId newLeafId = disk.allocatePage(mtr, index.leafSegment());
        RecordPage newLeaf = pageAccess.createIndexPage(mtr, newLeafId, index.indexId(), 0);
        IndexPageHandle newLeafHandle = pageAccess.openIndexPageHandle(mtr, newLeafId, PageLatchMode.EXCLUSIVE);

        oldLeaf.format(index.indexId(), 0);
        oldLeafHandle.writeSiblingLinks(oldLeafHeader.prevPageNo(), newLeafId.pageNo().value());
        newLeafHandle.writeSiblingLinks(oldLeafId.pageNo().value(), oldLeafHeader.nextPageNo());
        if (oldLeafHeader.nextPageNo() != FilePageHeader.FIL_NULL) {
            PageId rightSiblingId = PageId.of(oldLeafId.spaceId(), PageNo.of(oldLeafHeader.nextPageNo()));
            IndexPageHandle rightSibling = pageAccess.openIndexPageHandle(mtr, rightSiblingId, PageLatchMode.EXCLUSIVE);
            rightSibling.writeSiblingLinks(newLeafId.pageNo().value(), rightSibling.fileHeader().nextPageNo());
        }

        RecordRef insertedRef = insertAll(oldLeaf, oldLeafId, split.left(), index, inserted);
        RecordRef newInsertedRef = insertAll(newLeaf, newLeafId, split.right(), index, inserted);
        if (insertedRef == null) {
            insertedRef = newInsertedRef;
        }
        insertRootPointer(root, index, new BTreeNodePointer(lowKey(split.right(), index), newLeafId));
        return new BTreeInsertResult(index, insertedRef, index, true, List.of(newLeafId));
    }

    private IndexPageHandle openRoot(MiniTransaction mtr, BTreeIndex index, PageLatchMode mode) {
        if (mtr == null || index == null || mode == null) {
            throw new DatabaseValidationException("btree mtr/index/mode must not be null");
        }
        if (index.rootLevel() > 1) {
            throw new BTreeUnsupportedStructureException("split btree supports rootLevel 0 or 1 only: "
                    + index.rootLevel());
        }
        IndexPageHandle root = pageAccess.openIndexPageHandle(mtr, index.rootPageId(), mode);
        IndexPageHeader header = root.recordPage().header();
        if (header.indexId() != index.indexId()) {
            throw new BTreeStructureCorruptedException("root page index id mismatch: page="
                    + header.indexId() + " expected=" + index.indexId());
        }
        if (header.level() != index.rootLevel()) {
            throw new BTreeRootChangedException("root level changed: page=" + header.level()
                    + " snapshot=" + index.rootLevel());
        }
        return root;
    }

    private void validateLeafPage(RecordPage page, BTreeIndex index, PageId pageId) {
        IndexPageHeader header = page.header();
        if (header.indexId() != index.indexId()) {
            throw new BTreeStructureCorruptedException("leaf page index id mismatch at " + pageId
                    + ": page=" + header.indexId() + " expected=" + index.indexId());
        }
        if (header.level() != 0) {
            throw new BTreeStructureCorruptedException("expected leaf level 0 at " + pageId
                    + " but got " + header.level());
        }
    }

    private Optional<BTreeLookupResult> lookupInLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key) {
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return Optional.empty();
        }
        RecordCursor cursor = new RecordCursor(leaf, found.getAsInt(), index.schema(), registry);
        if (cursor.isDeleted()) {
            return Optional.empty();
        }
        return Optional.of(materialize(index, leafId, cursor));
    }

    private PageId chooseChild(RecordPage root, BTreeIndex index, SearchKey key) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        int prev = search.findInsertPosition(root, key, pointerSchema.keyDef(), pointerSchema.schema());
        int pointerOffset = prev == root.infimumOffset() ? root.nextRecord(prev) : prev;
        if (pointerOffset == root.supremumOffset()) {
            throw new BTreeStructureCorruptedException("non-leaf root has no child pointer for index "
                    + index.indexId());
        }
        RecordCursor cursor = new RecordCursor(root, pointerOffset, pointerSchema.schema(), registry);
        return pointerCodec.fromRecord(cursor.materialize(), pointerSchema).childPageId();
    }

    private void ensureUniqueAbsent(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key) {
        if (!index.unique()) {
            return;
        }
        if (search.findEqual(leaf, key, index.keyDef(), index.schema()).isPresent()) {
            throw new BTreeDuplicateKeyException("duplicate key in unique btree index " + index.indexId()
                    + " at leaf " + leafId);
        }
    }

    private boolean scanLeafPage(RecordPage leaf, PageId leafId, BTreeIndex index, BTreeScanRange range,
                                 ScanAccumulator acc) {
        for (int off : leaf.recordOffsetsInOrder()) {
            RecordCursor cursor = new RecordCursor(leaf, off, index.schema(), registry);
            if (cursor.isDeleted()) {
                continue;
            }
            if (belowLower(cursor, index, range)) {
                continue;
            }
            if (aboveUpper(cursor, index, range)) {
                return true;
            }
            acc.add(materialize(index, leafId, cursor));
            if (acc.full()) {
                return true;
            }
        }
        return false;
    }

    private List<LogicalRecord> materializeLeafRecords(RecordPage page, BTreeIndex index) {
        List<LogicalRecord> records = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            records.add(new RecordCursor(page, off, index.schema(), registry).materialize());
        }
        return records;
    }

    private List<LogicalRecord> sortedWithInserted(List<LogicalRecord> records, LogicalRecord inserted,
                                                   BTreeIndex index) {
        List<LogicalRecord> all = new ArrayList<>(records.size() + 1);
        all.addAll(records);
        all.add(inserted);
        all.sort(Comparator.comparing(rec -> keyOf(rec, index),
                (a, b) -> keyComparator.compare(a, b, index.keyDef(), index.schema())));
        return all;
    }

    private SplitRows splitRows(List<LogicalRecord> records) {
        if (records.size() < 2) {
            throw new BTreeStructureCorruptedException("cannot split fewer than two records");
        }
        int mid = records.size() >>> 1;
        return new SplitRows(List.copyOf(records.subList(0, mid)), List.copyOf(records.subList(mid, records.size())));
    }

    private RecordRef insertAll(RecordPage page, PageId pageId, List<LogicalRecord> rows, BTreeIndex index,
                                LogicalRecord inserted) {
        RecordRef insertedRef = null;
        for (LogicalRecord row : rows) {
            RecordRef ref = inserter.insert(page, pageId, row, index.keyDef(), index.schema());
            if (row == inserted) {
                insertedRef = ref;
            }
        }
        return insertedRef;
    }

    private void insertRootPointer(RecordPage root, BTreeIndex index, BTreeNodePointer pointer) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        LogicalRecord record = pointerCodec.toRecord(pointer, pointerSchema);
        inserter.insert(root, index.rootPageId(), record, pointerSchema.keyDef(), pointerSchema.schema());
    }

    private void ensureRootHasRoomForPointer(RecordPage root, BTreeIndex index, BTreeNodePointer pointer) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        LogicalRecord record = pointerCodec.toRecord(pointer, pointerSchema);
        int required = recordEncoder.encode(record, pointerSchema.schema()).length + 8;
        if (required > root.freeSpace()) {
            throw new BTreeParentSplitRequiredException("root page has no room for another node pointer in index "
                    + index.indexId());
        }
    }

    private SearchKey lowKey(List<LogicalRecord> rows, BTreeIndex index) {
        if (rows.isEmpty()) {
            throw new BTreeStructureCorruptedException("split child must not be empty");
        }
        return keyOf(rows.get(0), index);
    }

    private BTreeLookupResult materialize(BTreeIndex index, PageId pageId, RecordCursor cursor) {
        return new BTreeLookupResult(index, cursor.recordRef(pageId, index.indexId()), cursor.materialize());
    }

    private boolean belowLower(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        int c = recordComparator.compare(cursor, range.lowerKey(), index.keyDef(), index.schema());
        return c < 0 || (c == 0 && !range.lowerInclusive());
    }

    private boolean aboveUpper(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        int c = recordComparator.compare(cursor, range.upperKey(), index.keyDef(), index.schema());
        return c > 0 || (c == 0 && !range.upperInclusive());
    }

    private SearchKey keyOf(LogicalRecord record, BTreeIndex index) {
        List<ColumnValue> values = new ArrayList<>(index.keyDef().parts().size());
        for (var part : index.keyDef().parts()) {
            values.add(record.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(values);
    }

    private void requireLeafSegment(BTreeIndex index) {
        if (index.leafSegment() == null) {
            throw new BTreeUnsupportedStructureException("split-capable insert requires leaf segment metadata");
        }
    }

    /** 分裂后的左右记录集；两侧都必须非空。 */
    private record SplitRows(List<LogicalRecord> left, List<LogicalRecord> right) {
    }

    /** scan 结果累积器，集中管理 limit 与不可变输出。 */
    private static final class ScanAccumulator {
        private final int limit;
        private final List<BTreeLookupResult> rows = new ArrayList<>();

        private ScanAccumulator(int limit) {
            this.limit = limit;
        }

        private void add(BTreeLookupResult row) {
            rows.add(row);
        }

        private boolean full() {
            return rows.size() >= limit;
        }

        private List<BTreeLookupResult> results() {
            return List.copyOf(rows);
        }
    }
}
