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

    /**
     * 创建 {@code LeafOnlyBTreeIndexService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageAccess 由组合根提供的 {@code IndexPageAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code lookup} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code lookup} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code insert} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws BTreeDuplicateKeyException 目标身份或唯一键已被占用时抛出；调用方应回滚本次变更或改用其他合法身份
     * @throws BTreeSplitRequiredException 索引定位、结构修改或等待后重定位无法保持 B+Tree 不变量时抛出；调用方应释放 Guard 并回滚或重试
     */
    @Override
    public BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (record == null) {
            throw new DatabaseValidationException("btree insert record must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        RecordPage page = openRootLeaf(mtr, index, PageLatchMode.EXCLUSIVE);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        SearchKey key = keyOf(record, index);
        if (index.physicalUnique() && search.findEqual(page, key, index.keyDef(), index.schema()).isPresent()) {
            throw new BTreeDuplicateKeyException("duplicate key in unique btree index " + index.indexId());
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        try {
            return new BTreeInsertResult(index,
                    inserter.insert(page, index.rootPageId(), record, index.keyDef(), index.schema()));
        } catch (RecordPageOverflowException e) {
            throw new BTreeSplitRequiredException("root leaf page is full for index " + index.indexId(), e);
        }
    }

    /** 打开并校验 root leaf。非 leaf 在触页前拒绝；页 header 不匹配则说明元数据或页结构损坏。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code openRootLeaf} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws BTreeUnsupportedStructureException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     * @throws BTreeStructureCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private RecordPage openRootLeaf(MiniTransaction mtr, BTreeIndex index, PageLatchMode mode) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || index == null || mode == null) {
            throw new DatabaseValidationException("btree mtr/index/mode must not be null");
        }
        if (index.rootLevel() != 0) {
            throw new BTreeUnsupportedStructureException("leaf-only btree requires rootLevel=0: "
                    + index.rootLevel());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        RecordPage page = pageAccess.openIndexPage(mtr, index.rootPageId(), mode);
        IndexPageHeader header = page.header();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
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
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return page;
    }

    /** 将 RecordCursor 转成不持资源的结果对象；recordRef 使用 cursor 拷出的 header 信息。 */
    private BTreeLookupResult materialize(BTreeIndex index, RecordCursor cursor) {
        return new BTreeLookupResult(index, cursor.recordRef(index.rootPageId(), index.indexId()),
                cursor.materialize());
    }

    /** true 表示当前记录位于下界之前。 */
    private boolean belowLower(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        if (range.lowerBound().isEmpty()) {
            return false;
        }
        int c = comparator.compare(cursor, range.lowerBound().orElseThrow(), index.keyDef(), index.schema());
        return c < 0 || (c == 0 && !range.lowerInclusive());
    }

    /** true 表示当前记录已越过上界；record 链有序，调用方可停止扫描。 */
    private boolean aboveUpper(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        if (range.upperBound().isEmpty()) {
            return false;
        }
        int c = comparator.compare(cursor, range.upperBound().orElseThrow(), index.keyDef(), index.schema());
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
