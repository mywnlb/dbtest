package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeLeafTarget;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.BTreeSecondaryDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeSecondaryRemovalResult;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;

import java.util.List;

/**
 * DML、rollback 与 purge 共用的二级索引物理 mutation 决策点。它不改变事务锁/undo 顺序，只在逻辑动作已经
 * 决定后选择“全局 Change Buffer durable 接管”或“真实 B+Tree 直写”，从而避免三个调用方各自复制 eligibility。
 *
 * <p>并发边界：第一次 leaf 定位结束后释放所有页闩，再等待 per-target gate；gate 内用新只读 MTR 二次定位与
 * 容量检查，随后用独立写 MTR第三次稳定 child pointer并追加。Buffer Pool loader 对同一页持同一 gate，
 * 因而 mutation append 与发布前 merge 不会互相越过。gate 超时只回退直写，不遗留半条全局记录。</p>
 */
public final class SecondaryIndexMutationCoordinator {

    /** 单页最多允许的 pending mutation 数；与 128-page merge redo 预算配套，限制发布前延迟和单批日志上界。 */
    public static final int MAX_PENDING_PER_PAGE = 64;

    /** 当前引擎实例的 effective 策略、容量阈值和 gate timeout。 */
    private final ChangeBufferConfig config;
    /** 目标页 residency 的权威目录；eligibility 只接受未驻留页。 */
    private final BufferPool pool;
    /** 定位、append 和直写使用的短 MTR 工厂与 redo admission 入口。 */
    private final MiniTransactionManager mtrManager;
    /** 提供真实二级 B+Tree 定位、插入、标记和物理删除原语。 */
    private final SplitCapableBTreeIndexService btree;
    /** 结构修改后刷新调用方 root-level 快照，避免沿用陈旧描述符。 */
    private final BTreeRootSnapshotService rootSnapshots;
    /** 读取真实 leaf 连续空闲空间并服务直写后的 bitmap 修正。 */
    private final IndexPageAccess pageAccess;
    /** system.ibd 全局 mutation B+Tree；与 header 计数同一 MTR 更新。 */
    private final ChangeBufferStore store;
    /** 用户空间 4-bit/page eligibility 与 pending hint 仓储。 */
    private final ChangeBufferBitmapRepository bitmaps;
    /** 与 loader merge、worker、DDL drain 共用的 per-target 串行 gate。 */
    private final ChangeBufferPageGate pageGate;
    /** 保存跨延迟窗口解析 payload 所需的 exact-version 索引绑定。 */
    private final ChangeBufferMetadataCatalog metadataCatalog;
    /** 把逻辑二级 entry 编码成可跨重启验证的紧凑 payload。 */
    private final RecordEncoder recordEncoder;
    /** bitmap 公式与 mutation 单页大小边界使用的实例固定页大小。 */
    private final PageSize pageSize;
    /** 与 merger/DDL/worker 共享的进程级提交后计数 owner。 */
    private final ChangeBufferCounters counters;

    /**
     * @param config 运行期有效模式与有界等待/容量配置
     * @param pool 同一引擎 Buffer Pool，用于 O(1) residency 复核
     * @param mtrManager 同一 redo/access 域的短事务工厂
     * @param btree 普通二级树物理原语
     * @param rootSnapshots root level 刷新协作者
     * @param pageAccess 直写后读取 leaf 连续空闲空间的受控入口
     * @param store 全局 Change Buffer B+Tree
     * @param bitmaps 每空间 4-bit/page bitmap
     * @param pageGate buffer/merge/drain 共用目标页 gate
     * @param metadataCatalog exact-version merge 目录
     * @param registry 与目标二级页一致的 record codec 注册表
     * @param pageSize 实例固定页大小
     */
    public SecondaryIndexMutationCoordinator(
            ChangeBufferConfig config, BufferPool pool, MiniTransactionManager mtrManager,
            SplitCapableBTreeIndexService btree, BTreeRootSnapshotService rootSnapshots,
            IndexPageAccess pageAccess, ChangeBufferStore store,
            ChangeBufferBitmapRepository bitmaps, ChangeBufferPageGate pageGate,
            ChangeBufferMetadataCatalog metadataCatalog,
            TypeCodecRegistry registry, PageSize pageSize) {
        this(config, pool, mtrManager, btree, rootSnapshots, pageAccess, store, bitmaps,
                pageGate, metadataCatalog, registry, pageSize, new ChangeBufferCounters());
    }

    /**
     * 创建共享生产统计 owner 的二级 mutation 协调器。
     *
     * @param config 运行期 effective 策略
     * @param pool 目标页 residency 权威目录
     * @param mtrManager MTR/redo admission 入口
     * @param btree 普通二级树原语
     * @param rootSnapshots root level 刷新入口
     * @param pageAccess leaf 空闲空间读取入口
     * @param store 全局 mutation B+Tree
     * @param bitmaps 用户表空间 bitmap
     * @param pageGate 同目标页串行 gate
     * @param metadataCatalog exact-version 目录
     * @param registry record codec 注册表
     * @param pageSize 实例固定页大小
     * @param counters 引擎组合根共享统计 owner
     */
    public SecondaryIndexMutationCoordinator(
            ChangeBufferConfig config, BufferPool pool, MiniTransactionManager mtrManager,
            SplitCapableBTreeIndexService btree, BTreeRootSnapshotService rootSnapshots,
            IndexPageAccess pageAccess, ChangeBufferStore store,
            ChangeBufferBitmapRepository bitmaps, ChangeBufferPageGate pageGate,
            ChangeBufferMetadataCatalog metadataCatalog,
            TypeCodecRegistry registry, PageSize pageSize, ChangeBufferCounters counters) {
        if (config == null || pool == null || mtrManager == null || btree == null
                || rootSnapshots == null || pageAccess == null || store == null || bitmaps == null
                || pageGate == null || metadataCatalog == null
                || registry == null || pageSize == null || counters == null) {
            throw new DatabaseValidationException("secondary mutation coordinator dependencies must not be null");
        }
        this.config = config;
        this.pool = pool;
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.rootSnapshots = rootSnapshots;
        this.pageAccess = pageAccess;
        this.store = store;
        this.bitmaps = bitmaps;
        this.pageGate = pageGate;
        this.metadataCatalog = metadataCatalog;
        this.recordEncoder = new RecordEncoder(registry);
        this.pageSize = pageSize;
        this.counters = counters;
    }

    /**
     * 注册前台已经固定的完整表版本，供后续 demand-load merge 解析持久 identity。
     *
     * @param metadata 当前 DML 已固定的 exact-version 表/索引不可变快照
     * @throws ChangeBufferStateException 同一稳定三元组已绑定不同物理 descriptor/layout 时抛出
     */
    public void register(TableIndexMetadata metadata) {
        metadataCatalog.register(metadata);
    }

    /**
     * 发布新 entry 或 revive 同一 marked identity；两者的延迟表示均为 INSERT，merge 会按当前页状态幂等收敛。
     *
     * @param tableId exact-version 表 id
     * @param schemaVersion exact-version schema 版本
     * @param metadata 目标非聚簇索引 metadata
     * @param entry 完整紧凑二级 entry；deleted 位会归一化为 false
     * @param revive true 表示直写时只取消 delete mark，false 表示真实插入
     * @param directPurpose 回退直写使用的调用方 redo purpose
     * @return durable buffered 或 direct 完成态
     */
    public SecondaryIndexMutationResult insertOrRevive(
            long tableId, long schemaVersion, SecondaryIndexMetadata metadata,
            LogicalRecord entry, boolean revive, RedoBudgetPurpose directPurpose) {
        LogicalRecord normalized = normalizeEntry(metadata, entry);
        BTreeIndex index = refresh(metadata.index());
        SecondaryIndexMutationResult buffered = tryBuffer(tableId, schemaVersion, metadata, index,
                normalized, ChangeBufferOperation.INSERT);
        if (buffered != null) {
            return buffered;
        }
        return directInsertOrRevive(index, metadata, normalized, revive, directPurpose);
    }

    /**
     * 翻转二级 entry delete 位；revive 的延迟表示使用 INSERT，delete-mark 使用 DELETE_MARK。
     *
     * @param tableId exact-version 表 id
     * @param schemaVersion exact-version schema 版本
     * @param metadata 目标非聚簇索引 metadata
     * @param entry 由完整 physical key 重建的紧凑 entry
     * @param deleted 目标 delete 位
     * @param directPurpose 回退直写 redo purpose
     * @return durable buffered 或 direct mark 完成态
     */
    public SecondaryIndexMutationResult setDeleteMark(
            long tableId, long schemaVersion, SecondaryIndexMetadata metadata,
            LogicalRecord entry, boolean deleted, RedoBudgetPurpose directPurpose) {
        LogicalRecord normalized = normalizeEntry(metadata, entry);
        BTreeIndex index = refresh(metadata.index());
        ChangeBufferOperation operation = deleted
                ? ChangeBufferOperation.DELETE_MARK : ChangeBufferOperation.INSERT;
        SecondaryIndexMutationResult buffered = tryBuffer(tableId, schemaVersion, metadata, index,
                normalized, operation);
        if (buffered != null) {
            return buffered;
        }
        return directSetDeleteMark(index, metadata, normalized, deleted, directPurpose);
    }

    /**
     * 物理删除二级 entry。buffered DELETE 不在 merge 时执行 leaf merge；直写仍复用既有结构删除与前态校验。
     *
     * @param tableId exact-version 表 id
     * @param schemaVersion exact-version schema 版本
     * @param metadata 目标非聚簇索引 metadata
     * @param entry 完整紧凑二级 entry
     * @param deleteMode rollback/purge 的直写前态约束
     * @param directPurpose 回退直写 redo purpose
     * @return durable buffered 或 direct removal 完成态
     */
    public SecondaryIndexMutationResult delete(
            long tableId, long schemaVersion, SecondaryIndexMetadata metadata,
            LogicalRecord entry, SecondaryIndexDeleteMode deleteMode,
            RedoBudgetPurpose directPurpose) {
        if (deleteMode == null) {
            throw new DatabaseValidationException("secondary delete mode must not be null");
        }
        LogicalRecord normalized = normalizeEntry(metadata, entry);
        BTreeIndex index = refresh(metadata.index());
        SecondaryIndexMutationResult buffered = tryBuffer(tableId, schemaVersion, metadata, index,
                normalized, ChangeBufferOperation.DELETE);
        if (buffered != null) {
            return buffered;
        }
        return directDelete(index, metadata, normalized, deleteMode, directPurpose);
    }

    /**
     * 尝试把动作交给 Change Buffer；返回 null 表示任一可预期 eligibility 条件不成立，应无条件直写。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证模式、索引属性与 identity，编码 entry 后用短读 MTR只定位 leaf identity并释放所有页闩。</li>
     *     <li>确认目标未驻留后有界取得 page gate；gate 内再次检查 residency、路由、bitmap、单页和全局容量。</li>
     *     <li>用独立写 MTR第三次稳定 level-1 pointer，在显式反序作用域内追加全局记录并设置 buffered bit。</li>
     *     <li>提交后返回接管态；gate timeout、路由/驻留/容量变化只返回直写，格式损坏和提交失败向上抛出。</li>
     * </ol>
     *
     * @param tableId exact-version 正表 id，来自当前已固定 DD 快照
     * @param schemaVersion entry 编码所用的正 schema 版本
     * @param metadata 非唯一普通二级索引 metadata；唯一/降序等不合格属性返回 direct
     * @param index 已刷新 root level 的物理 descriptor
     * @param entry 已归一化为 live conventional 的完整二级 entry
     * @param operation 本次希望延迟的 INSERT、DELETE_MARK 或 DELETE
     * @return durable append 后的 BUFFERED 结果；任一优化条件不成立时返回 {@code null} 表示调用方必须直写
     * @throws ChangeBufferFormatException header/tree/bitmap 持久证据损坏时抛出，不能降级掩盖损坏
     */
    private SecondaryIndexMutationResult tryBuffer(
            long tableId, long schemaVersion, SecondaryIndexMetadata metadata, BTreeIndex index,
            LogicalRecord entry, ChangeBufferOperation operation) {
        // 1、纯属性拒绝早于任何页访问；逻辑唯一、DESC、聚簇和内部树永远不能推迟唯一/排序语义。
        requireIdentity(tableId, schemaVersion, metadata, entry, operation);
        if (!config.mode().allows(operation) || metadata.logicalUnique()
                || index.clustered() || index.pageType() != PageType.INDEX
                || index.rootPageId().spaceId().equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                || index.keyDef().parts().stream().anyMatch(part -> part.order() != KeyOrder.ASC)) {
            return null;
        }
        byte[] entryBytes = recordEncoder.encode(entry, index.schema());
        SearchKey key = metadata.layout().physicalKey(entry);
        BTreeLeafTarget first = locate(index, key);
        if (first.rootLeaf() || pool.isResident(first.pageId()) || !bitmaps.supportsTarget(first.pageId())) {
            return null;
        }

        // 2、等待 gate 时已经没有 MTR/page latch；超时是优化失败而非 DML 失败，安全回退普通 B+Tree。
        ChangeBufferPageGate.Lease lease;
        try {
            lease = pageGate.acquire(first.pageId(), config.pageGateTimeout());
        } catch (ChangeBufferPageGateTimeoutException timeout) {
            return null;
        }
        try (lease) {
            if (pool.isResident(first.pageId()) || !bitmaps.supportsTarget(first.pageId())) {
                return null;
            }
            EligibilitySnapshot eligibility = inspectEligibility(index, key, first.pageId(), operation, entryBytes);
            if (!eligibility.allowed()) {
                return null;
            }

            // 3、scan 的 S latch 已释放；写 MTR重新稳定 pointer，避免 S→X 升级并允许其它目标并发追加。
            MiniTransaction append = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.CHANGE_BUFFER_APPEND));
            try {
                BTreeLeafTarget finalTarget = btree.locateLeafWithoutLoading(append, index, key);
                if (finalTarget.rootLeaf() || !finalTarget.pageId().equals(first.pageId())
                        || pool.isResident(first.pageId()) || !bitmaps.supportsTarget(first.pageId())) {
                    mtrManager.rollbackUncommitted(append);
                    return null;
                }
                try (var order = append.allowOutOfOrderPageLatch(
                        "change-buffer append holds user level-1 parent S, then accesses system ibuf and user bitmap; "
                                + "target loader takes the same page gate and never waits for this parent in reverse")) {
                    store.requireAppendCapacity(append,
                            config.maxBufferedPageEquivalents(pool.capacity()));
                    store.append(append, first.pageId(), tableId, schemaVersion,
                            index.indexId(), operation, entryBytes);
                    ChangeBufferBitmapState bitmap = eligibility.bitmap();
                    bitmaps.write(append, first.pageId(), new ChangeBufferBitmapState(
                            bitmap.freeSpaceClass(), true, bitmap.changeBufferInternal()));
                }
                // 4、全局 tree/header/bitmap 共享一个 redo group；只有 commit 后才报告 BUFFERED。
                mtrManager.commit(append);
                counters.recordBuffered();
                return SecondaryIndexMutationResult.buffered(operation, first.pageId());
            } catch (ChangeBufferCapacityExceededException capacityRace) {
                rollbackActive(append, capacityRace);
                return null;
            } catch (RuntimeException failure) {
                rollbackActive(append, failure);
                throw failure;
            }
        }
    }

    /**
     * 在目标 gate 内重新物化 eligibility，并在返回前释放 parent/header/tree/bitmap 全部 S latch。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>重新只读路由并复核 target/residency，竞争导致 identity 漂移时发布 rejected 快照。</li>
     *     <li>在局部越序证明内读取全局容量、同页 mutation 和 bitmap，INSERT 额外扣除全部 pending INSERT 预留。</li>
     *     <li>提交只读 MTR 后返回不持资源的不可变结果；失败则回滚全部 fix/latch，持久证据不改变。</li>
     * </ol>
     *
     * @param index 已刷新 descriptor
     * @param key 完整 physical key
     * @param expected 第一次路由得到、且当前 gate 对应的目标页
     * @param operation 待缓冲物理动作
     * @param entryBytes 完整二级 entry 编码，INSERT 用于容量预留
     * @return 不引用任何 guard 的 allow/bitmap 快照
     */
    private EligibilitySnapshot inspectEligibility(BTreeIndex index, SearchKey key, PageId expected,
                                                   ChangeBufferOperation operation, byte[] entryBytes) {
        // 1、只读重定位排除 gate 获取期间发生的 SMO、载入或 root 变化。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            BTreeLeafTarget current = btree.locateLeafWithoutLoading(read, index, key);
            if (current.rootLeaf() || !current.pageId().equals(expected) || pool.isResident(expected)) {
                mtrManager.commit(read);
                return EligibilitySnapshot.rejected();
            }
            // 2、读取全部 eligibility 证据；系统/bitmap 资源不逃逸到 append 写 MTR。
            EligibilitySnapshot snapshot;
            try (var order = read.allowOutOfOrderPageLatch(
                    "change-buffer eligibility holds user parent S and reads system header/tree plus user bitmap; "
                            + "the per-target gate excludes a loader from acquiring the reverse dependency")) {
                boolean globalCapacity = withinGlobalCapacity(read);
                List<ChangeBufferMutation> existing = store.scanPage(
                        read, expected, MAX_PENDING_PER_PAGE + 1);
                ChangeBufferBitmapState bitmap = bitmaps.read(read, expected);
                if (bitmap.changeBufferInternal() || existing.size() >= MAX_PENDING_PER_PAGE
                        || !globalCapacity) {
                    snapshot = EligibilitySnapshot.rejected();
                } else if (operation == ChangeBufferOperation.INSERT) {
                    long reserved = existing.stream()
                            .filter(mutation -> mutation.operation() == ChangeBufferOperation.INSERT)
                            .mapToLong(mutation -> (long) mutation.entryBytes().length + Short.BYTES)
                            .sum();
                    long available = ChangeBufferBitmapLayout.freeSpaceLowerBound(
                            pageSize, bitmap.freeSpaceClass());
                    if (reserved + entryBytes.length + Short.BYTES > available) {
                        snapshot = EligibilitySnapshot.rejected();
                    } else {
                        snapshot = new EligibilitySnapshot(true, bitmap);
                    }
                } else {
                    snapshot = new EligibilitySnapshot(true, bitmap);
                }
            }
            // 3、提交只负责释放只读资源，返回值已经完全物化。
            mtrManager.commit(read);
            return snapshot;
        } catch (RuntimeException failure) {
            rollbackActive(read, failure);
            throw failure;
        }
    }

    /**
     * 按 Buffer Pool frame 数落实 max-size 百分比，并采用“每条 pending 最坏占一整页”的保守计费；这与
     * MySQL 的配置基准一致，但会比其按 Change Buffer 实际页占用的核算更早回退直写。
     *
     * @param read 当前 eligibility 只读 MTR，用于取得与同次全局树扫描一致的 pending 计数
     * @return 当前 pending 页等价量仍低于配置上限时为 {@code true}
     */
    private boolean withinGlobalCapacity(MiniTransaction read) {
        long allowedPageEquivalents = config.maxBufferedPageEquivalents(pool.capacity());
        return store.pendingOperations(read) < allowedPageEquivalents;
    }

    /**
     * 新插入/复活的直写分支；同一 MTR 重算实际 leaf 空闲等级并保持 buffered 位。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先只读定位诊断 target并按当前 root height 申请结构或点改 redo 预算。</li>
     *     <li>执行 revive 或真实 insert；insert 用返回的实际 leaf identity 修正受管理 bitmap。</li>
     *     <li>提交后发布 direct counter/result；异常回滚 MTR，不能把未提交页修改报告完成。</li>
     * </ol>
     *
     * @param index 已刷新二级 descriptor
     * @param metadata key/layout metadata
     * @param entry 待发布 live entry
     * @param revive 是否只取消现有 entry 的 delete mark
     * @param purpose 调用方 redo 用途
     * @return 携带 B+Tree 物理结果的 direct 完成态
     */
    private SecondaryIndexMutationResult directInsertOrRevive(
            BTreeIndex index, SecondaryIndexMetadata metadata, LogicalRecord entry,
            boolean revive, RedoBudgetPurpose purpose) {
        // 1、定位只用于结果 target；写 MTR 使用与真实动作匹配的 workload 上界。
        SearchKey key = metadata.layout().physicalKey(entry);
        PageId expected = locate(index, key).pageId();
        MiniTransaction write = mtrManager.begin(mtrManager.budgetFor(purpose,
                revive ? BTreeRedoBudgetEstimator.pointRewrite()
                        : BTreeRedoBudgetEstimator.insert(index.rootLevel())));
        try {
            // 2、revive 不改变空间；真实 insert 以结构后置 leaf 重算 bitmap，范围外页保持 direct-only。
            if (revive) {
                BTreeSecondaryDeleteMarkResult result = btree.setSecondaryDeleteMark(write, index, key, false);
                // 3、点改同样只在 redo/pageLSN/dirty 发布完成后增加 direct counter。
                mtrManager.commit(write);
                counters.recordDirectFallback();
                return SecondaryIndexMutationResult.marked(
                        ChangeBufferOperation.INSERT, expected, result);
            }
            BTreeInsertResult result = btree.insertSecondary(write, index, entry);
            PageId actual = result.recordRef().pageId();
            updateBitmapAfterDirect(write, actual, false);
            // 3、只有 redo/pageLSN/dirty 发布完成后才增加 direct counter。
            mtrManager.commit(write);
            counters.recordDirectFallback();
            return SecondaryIndexMutationResult.inserted(actual, result);
        } catch (RuntimeException failure) {
            rollbackActive(write, failure);
            throw failure;
        }
    }

    /**
     * delete-mark/revive 的直写分支；动作不改变树形，定位 leaf identity 在整个 MTR 内稳定。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>物化完整 physical key 与诊断 target，并在任何写页前申请点改 redo 预算。</li>
     *     <li>在一个写 MTR 中达到指定 delete-mark 状态，不触发树形或 free-space 变化。</li>
     *     <li>提交后发布 direct 计数/result；异常回滚并保留 B+Tree 状态分类。</li>
     * </ol>
     *
     * @param index 已刷新二级 descriptor
     * @param metadata key/layout metadata
     * @param entry 待翻转状态的完整二级 entry
     * @param deleted 目标 delete-mark 状态
     * @param purpose 调用方 redo 用途
     * @return 携带点改状态的 direct 完成态
     */
    private SecondaryIndexMutationResult directSetDeleteMark(
            BTreeIndex index, SecondaryIndexMetadata metadata, LogicalRecord entry,
            boolean deleted, RedoBudgetPurpose purpose) {
        // 1、只读定位在写 MTR 前完成，避免预算阶段持有 root S latch。
        SearchKey key = metadata.layout().physicalKey(entry);
        PageId expected = locate(index, key).pageId();
        MiniTransaction write = mtrManager.begin(mtrManager.budgetFor(
                purpose, BTreeRedoBudgetEstimator.pointRewrite()));
        try {
            // 2、点改不重算 free class；record 长度和树形均不变化。
            BTreeSecondaryDeleteMarkResult result = btree.setSecondaryDeleteMark(write, index, key, deleted);
            // 3、durable commit 是 direct 完成态的唯一发布边界。
            mtrManager.commit(write);
            counters.recordDirectFallback();
            return SecondaryIndexMutationResult.marked(deleted
                    ? ChangeBufferOperation.DELETE_MARK : ChangeBufferOperation.INSERT,
                    expected, result);
        } catch (RuntimeException failure) {
            rollbackActive(write, failure);
            throw failure;
        }
    }

    /**
     * 结构删除直写分支；被释放页和删除后重新路由得到的存活 leaf bitmap 都降为保守值，避免 merge survivor
     * 或页号复用继承过期乐观空间等级。v1 未管理的跨区间页只走真实 B+Tree，不触碰未预留 bitmap。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按当前 root height 申请结构删除预算并执行 published-live 或 delete-marked 物理删除。</li>
     *     <li>把旧目标与全部 freed identity 的受管理 bitmap 清成保守值，页号复用不得继承旧 pending/free 状态。</li>
     *     <li>使用结构后置 descriptor 重新路由同一 key，把 merge/redistribute 后真正存活的 leaf 也降为等级 0。</li>
     *     <li>提交后发布 direct 统计与结构结果；任一步失败回滚当前 MTR并保留调用方重试语义。</li>
     * </ol>
     *
     * @param index 已刷新二级 descriptor
     * @param metadata key/layout metadata
     * @param entry 待物理摘除的完整二级 entry
     * @param mode 调用方要求的 live 或 delete-marked 前态
     * @param purpose rollback 或 purge 对应 redo 用途
     * @return 携带后置 descriptor/freed pages 的 direct 完成态
     */
    private SecondaryIndexMutationResult directDelete(
            BTreeIndex index, SecondaryIndexMetadata metadata, LogicalRecord entry,
            SecondaryIndexDeleteMode mode, RedoBudgetPurpose purpose) {
        SearchKey key = metadata.layout().physicalKey(entry);
        PageId expected = locate(index, key).pageId();
        MiniTransaction write = mtrManager.begin(mtrManager.budgetFor(
                purpose, BTreeRedoBudgetEstimator.structuralDelete(index.rootLevel())));
        try {
            // 1、B+Tree 返回后置 root level 与 freed pages；状态冲突仍交调用方按 rollback/purge 语义分类。
            BTreeSecondaryRemovalResult result = mode == SecondaryIndexDeleteMode.PUBLISHED_LIVE
                    ? btree.deletePublishedSecondary(write, index, key)
                    : btree.purgeDeleteMarkedSecondary(write, index, key);
            // 2、删除后先使旧目标与已释放 identity 保守；跨 v1 bitmap 范围的页明确跳过。
            updateBitmapConservatively(write, expected, 0);
            for (PageId freed : result.freedPages()) {
                if (!bitmaps.supportsTarget(freed)) {
                    continue;
                }
                try (var order = write.allowOutOfOrderPageLatch(
                        "secondary delete clears bitmap after FSP freed the page; no data-page latch is reacquired")) {
                    bitmaps.write(write, freed, new ChangeBufferBitmapState(0, false, false));
                }
            }
            // 3、结构删除可能把 victim 合并进左 sibling；后置路由确保真正存活页也不保留乐观等级。
            BTreeLeafTarget survivor = btree.locateLeafWithoutLoading(write, result.indexAfter(), key);
            updateBitmapConservatively(write, survivor.pageId(), 0);
            // 4、bitmap 与 B+Tree 结构共享提交边界，成功后再发布进程内计数。
            mtrManager.commit(write);
            counters.recordDirectFallback();
            return SecondaryIndexMutationResult.removed(expected, result);
        } catch (RuntimeException failure) {
            rollbackActive(write, failure);
            throw failure;
        }
    }

    /** 在目标 leaf 仍由写 MTR 持有时读取真实连续空闲空间，并在同一提交边界更新 bitmap。 */
    private void updateBitmapAfterDirect(MiniTransaction write, PageId pageId, boolean internal) {
        if (!bitmaps.supportsTarget(pageId)) {
            return;
        }
        int freeBytes = pageAccess.openIndexPage(write, pageId, PageLatchMode.EXCLUSIVE).freeSpace();
        try (var order = write.allowOutOfOrderPageLatch(
                "direct secondary mutation updates lower-numbered ibuf bitmap after the target leaf; "
                        + "all production target writers share B+Tree X latch and bitmap never acquires a target latch")) {
            ChangeBufferBitmapState old = bitmaps.readForUpdate(write, pageId);
            bitmaps.write(write, pageId, new ChangeBufferBitmapState(
                    ChangeBufferBitmapLayout.freeSpaceClass(pageSize, freeBytes),
                    old.buffered(), internal || old.changeBufferInternal()));
        }
    }

    /** 结构删除无法稳定返回存活 leaf identity 时只发布 0 等级，并保留已有 pending/internal 位。 */
    private void updateBitmapConservatively(MiniTransaction write, PageId pageId, int freeClass) {
        if (!bitmaps.supportsTarget(pageId)) {
            return;
        }
        try (var order = write.allowOutOfOrderPageLatch(
                "structural secondary delete writes a conservative bitmap value after B+Tree page latches; "
                        + "bitmap code never opens or waits for an index page")) {
            ChangeBufferBitmapState old = bitmaps.readForUpdate(write, pageId);
            bitmaps.write(write, pageId, new ChangeBufferBitmapState(
                    freeClass, old.buffered(), old.changeBufferInternal()));
        }
    }

    /** 只读刷新 root level，返回前释放 root S latch，避免 redo admission 使用过期高度。 */
    private BTreeIndex refresh(BTreeIndex index) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            BTreeIndex result = rootSnapshots.refresh(read, index);
            mtrManager.commit(read);
            return result;
        } catch (RuntimeException failure) {
            rollbackActive(read, failure);
            throw failure;
        }
    }

    /** 首次 eligibility 定位；返回前释放全部 root/internal S latch。 */
    private BTreeLeafTarget locate(BTreeIndex index, SearchKey key) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            BTreeLeafTarget result = btree.locateLeafWithoutLoading(read, index, key);
            mtrManager.commit(read);
            return result;
        } catch (RuntimeException failure) {
            rollbackActive(read, failure);
            throw failure;
        }
    }

    private static LogicalRecord normalizeEntry(SecondaryIndexMetadata metadata, LogicalRecord entry) {
        if (metadata == null || entry == null) {
            throw new DatabaseValidationException("secondary mutation metadata/entry must not be null");
        }
        metadata.layout().physicalKey(entry);
        return new LogicalRecord(entry.schemaVersion(), entry.columnValues(), false,
                RecordType.CONVENTIONAL, null);
    }

    private static void requireIdentity(long tableId, long schemaVersion, SecondaryIndexMetadata metadata,
                                        LogicalRecord entry, ChangeBufferOperation operation) {
        if (tableId <= 0 || schemaVersion <= 0 || metadata == null || entry == null || operation == null
                || metadata.index().schema().schemaVersion() != schemaVersion) {
            throw new DatabaseValidationException("secondary change buffer identity/entry is invalid");
        }
    }

    private void rollbackActive(MiniTransaction mtr, RuntimeException primary) {
        if (mtr != null && mtr.state() == MiniTransactionState.ACTIVE) {
            try {
                mtrManager.rollbackUncommitted(mtr);
            } catch (RuntimeException releaseFailure) {
                primary.addSuppressed(releaseFailure);
            }
        }
    }

    /** gate 内已经冻结的 bitmap 与 allow/reject 结果。 */
    private record EligibilitySnapshot(boolean allowed, ChangeBufferBitmapState bitmap) {
        private static EligibilitySnapshot rejected() {
            return new EligibilitySnapshot(false, new ChangeBufferBitmapState(0, false, false));
        }
    }
}
