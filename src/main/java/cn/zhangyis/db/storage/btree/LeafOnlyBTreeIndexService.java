package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.IndexPageHeader;
import cn.zhangyis.db.storage.record.page.RecordComparator;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPageKeyOrderValidator;
import cn.zhangyis.db.storage.record.page.RecordPageOverflowException;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * B1/B2 的 leaf-only B+Tree 实现。该实现把 root page 当作唯一 leaf 页，复用 record.page 的页内查找、
 * 比较和物化能力，只负责 B+Tree 层的元数据校验、MTR-owned page 打开以及结果封装。
 *
 * <p>边界：不支持 non-leaf root、sibling scan、split/merge、事务锁等待、MVCC 或 undo。遇到这些结构时必须显式拒绝，
 * 避免调用方误以为已经具备完整 B+Tree 语义。
 */
public final class LeafOnlyBTreeIndexService implements BTreeIndexService {

    /** INDEX 页 MTR facade，负责以 S/X latch 打开 root page，且不暴露 BufferFrame。 */
    private final IndexPageAccess pageAccess;
    /** 类型 codec 注册表；用于页内比较和物化。 */
    private final TypeCodecRegistry registry;
    /** 页内相等查找器；不持状态，复用 record.page 的目录二分和 next_record 线扫。 */
    private final RecordPageSearch search;
    /** 页内记录与 SearchKey 的比较器；scan 边界判断用。 */
    private final RecordComparator comparator;
    /** 页内 key 有序插入器；B+Tree 第一片只负责调用与异常边界。 */
    private final RecordPageInserter inserter;
    /** 既有 leaf 页的 schema-aware 用户链校验器；物理结构校验后、业务查找或写入前执行。 */
    private final RecordPageKeyOrderValidator keyOrderValidator;

    public LeafOnlyBTreeIndexService(IndexPageAccess pageAccess, TypeCodecRegistry registry) {
        if (pageAccess == null || registry == null) {
            throw new DatabaseValidationException("leaf btree pageAccess/registry must not be null");
        }
        this.pageAccess = pageAccess;
        this.registry = registry;
        this.search = new RecordPageSearch(registry);
        this.comparator = new RecordComparator(registry);
        this.inserter = new RecordPageInserter(registry);
        this.keyOrderValidator = new RecordPageKeyOrderValidator(registry);
    }

    /**
     * 点查 root leaf。数据流：校验输入与 leaf-only 结构 → S latch 打开 root → 校验页 header →
     * record.page 查等值 offset → 在 latch 持有期间物化记录 → 返回无资源持有的结果对象。
     */
    @Override
    public Optional<BTreeLookupResult> lookup(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        if (key == null) {
            throw new DatabaseValidationException("btree lookup key must not be null");
        }
        RecordPage page = openRootLeaf(mtr, index, PageLatchMode.SHARED);
        OptionalInt found = search.findEqual(page, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return Optional.empty();
        }
        RecordCursor cursor = new RecordCursor(page, found.getAsInt(), index.schema(), registry);
        if (cursor.isDeleted()) {
            return Optional.empty();
        }
        return Optional.of(materialize(index, cursor));
    }

    /**
     * 单页 bounded scan。数据流：校验输入与 leaf-only 结构 → S latch 打开 root → 按 record 链顺序遍历 →
     * 用 RecordComparator 判断 lower/upper 边界 → 在 latch 持有期间物化结果。由于当前没有 sibling link，
     * 扫描不会跨页，后续 split 片会扩展为 leaf sibling traversal。
     */
    @Override
    public List<BTreeLookupResult> scanLeaf(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        if (range == null) {
            throw new DatabaseValidationException("btree scan range must not be null");
        }
        RecordPage page = openRootLeaf(mtr, index, PageLatchMode.SHARED);
        if (range.limit() == 0) {
            return List.of();
        }
        List<BTreeLookupResult> rows = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            RecordCursor cursor = new RecordCursor(page, off, index.schema(), registry);
            if (cursor.isDeleted()) {
                continue;
            }
            if (belowLower(cursor, index, range)) {
                continue;
            }
            if (aboveUpper(cursor, index, range)) {
                break;
            }
            rows.add(materialize(index, cursor));
            if (rows.size() >= range.limit()) {
                break;
            }
        }
        return List.copyOf(rows);
    }

    /**
     * 在 root leaf 中插入一条记录。数据流：校验输入与 leaf-only 结构 → X latch 打开 root →
     * unique 索引做物理重复 key 检查 → 调 RecordPageInserter 改页。实际 redo 由调用方 MTR-owned PageGuard 收集。
     */
    @Override
    public BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("btree insert record must not be null");
        }
        RecordPage page = openRootLeaf(mtr, index, PageLatchMode.EXCLUSIVE);
        SearchKey key = keyOf(record, index);
        if (index.unique() && search.findEqual(page, key, index.keyDef(), index.schema()).isPresent()) {
            throw new BTreeDuplicateKeyException("duplicate key in unique btree index " + index.indexId());
        }
        try {
            return new BTreeInsertResult(index,
                    inserter.insert(page, index.rootPageId(), record, index.keyDef(), index.schema()));
        } catch (RecordPageOverflowException e) {
            throw new BTreeSplitRequiredException("root leaf page is full for index " + index.indexId(), e);
        }
    }

    /** 打开并校验 root leaf。非 leaf 在触页前拒绝；页 header 不匹配则说明元数据或页结构损坏。 */
    private RecordPage openRootLeaf(MiniTransaction mtr, BTreeIndex index, PageLatchMode mode) {
        if (mtr == null || index == null || mode == null) {
            throw new DatabaseValidationException("btree mtr/index/mode must not be null");
        }
        if (index.rootLevel() != 0) {
            throw new BTreeUnsupportedStructureException("leaf-only btree requires rootLevel=0: "
                    + index.rootLevel());
        }
        RecordPage page = pageAccess.openIndexPage(mtr, index.rootPageId(), mode);
        IndexPageHeader header = page.header();
        if (header.indexId() != index.indexId()) {
            throw new BTreeStructureCorruptedException("root page index id mismatch: page="
                    + header.indexId() + " expected=" + index.indexId());
        }
        if (header.level() != 0) {
            throw new BTreeUnsupportedStructureException("leaf-only btree cannot read page level "
                    + header.level());
        }
        keyOrderValidator.validate(index.rootPageId(), page, index.schema(), index.keyDef(),
                RecordType.CONVENTIONAL);
        return page;
    }

    /** 将 RecordCursor 转成不持资源的结果对象；recordRef 使用 cursor 拷出的 header 信息。 */
    private BTreeLookupResult materialize(BTreeIndex index, RecordCursor cursor) {
        return new BTreeLookupResult(index, cursor.recordRef(index.rootPageId(), index.indexId()),
                cursor.materialize());
    }

    /** true 表示当前记录位于下界之前。 */
    private boolean belowLower(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        int c = comparator.compare(cursor, range.lowerKey(), index.keyDef(), index.schema());
        return c < 0 || (c == 0 && !range.lowerInclusive());
    }

    /** true 表示当前记录已越过上界；record 链有序，调用方可停止扫描。 */
    private boolean aboveUpper(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        int c = comparator.compare(cursor, range.upperKey(), index.keyDef(), index.schema());
        return c > 0 || (c == 0 && !range.upperInclusive());
    }

    /** 按 index.keyDef 的 key part 从逻辑记录抽取 SearchKey；列值顺序仍由 TableSchema/IndexKeyDef 负责约束。 */
    private SearchKey keyOf(LogicalRecord record, BTreeIndex index) {
        List<cn.zhangyis.db.storage.record.type.ColumnValue> values = new ArrayList<>(index.keyDef().parts().size());
        for (var part : index.keyDef().parts()) {
            values.add(record.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(values);
    }
}
