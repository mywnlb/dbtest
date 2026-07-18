package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.SegmentDropPlan;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.TablespaceDrainResult;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.sdi.SdiPageRepository;
import cn.zhangyis.db.storage.sdi.SdiIndexBuildDescriptor;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * storage.api 的物理 DDL Facade。CREATE 负责 GENERAL tablespace/FSP/segment/index root，DROP 负责独占准入、
 * WAL-safe drain、持久 DISCARDED marker、Buffer Pool 失效、关闭句柄和删除文件；DD/MDL 状态由上层协调器拥有。
 */
@Slf4j
public final class TableDdlStorageService {

    private final MiniTransactionManager mtrManager;
    private final DiskSpaceManager disk;
    private final IndexPageAccess indexPages;
    private final BufferPool pool;
    private final PageStore store;
    private final FlushService flush;
    private final TablespaceAccessController accessController;
    /** 固定 page3 SDI 物理仓储；只处理 opaque payload 和页级完整性。 */
    private final SdiPageRepository sdiPages;
    private final StorageTableSchemaMapper schemaMapper = new StorageTableSchemaMapper();
    /** CREATE INDEX backfill 复用生产 B+Tree scan/insert，不另写页算法。 */
    private final SplitCapableBTreeIndexService btree;
    /** 每行写 MTR 前从稳定 root 页刷新层级，避免低估 split redo。 */
    private final BTreeRootSnapshotService rootSnapshots;
    /** 稳定 storage DTO/binding 到聚簇/二级 exact-version descriptor 的唯一工厂。 */
    private final BTreeIndexMetadataFactory indexMetadataFactory = new BTreeIndexMetadataFactory();

    public TableDdlStorageService(MiniTransactionManager mtrManager, DiskSpaceManager disk,
                                  IndexPageAccess indexPages, BufferPool pool, PageStore store,
                                  FlushService flush, TablespaceAccessController accessController,
                                  PageSize pageSize, SplitCapableBTreeIndexService btree,
                                  BTreeRootSnapshotService rootSnapshots) {
        if (mtrManager == null || disk == null || indexPages == null || pool == null || store == null
                || flush == null || accessController == null || pageSize == null
                || btree == null || rootSnapshots == null) {
            throw new DatabaseValidationException("table DDL storage collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.disk = disk;
        this.indexPages = indexPages;
        this.pool = pool;
        this.store = store;
        this.flush = flush;
        this.accessController = accessController;
        this.sdiPages = new SdiPageRepository(pool, pageSize);
        this.btree = btree;
        this.rootSnapshots = rootSnapshots;
    }

    /**
     * 创建可由上层数据字典发布并立即打开的物理表。
     *
     * <p>本方法只负责 {@code storage.api} 范围内的物理 DDL，不获取 MDL，也不发布 {@code ACTIVE}
     * 字典版本；调用方必须先独占目标表名，并且只能在本方法成功返回 binding 后提交字典事务。</p>
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在产生文件、页或 redo 副作用前，校验请求并把每个索引映射为 record schema/key，确保列类型、
     *     key part 和聚簇索引布局能够被存储层解释；同时判断该表是否需要共享的表级 LOB segment。</li>
     *     <li>按 page0/FSP、每索引 leaf/non-leaf segment、root page 以及可选 LOB segment 的最坏页镜像数
     *     申请 DDL redo admission 预算；算术溢出或预算不足时不得创建 tablespace。</li>
     *     <li>在同一个 MTR 中创建 GENERAL tablespace，并为每个索引分配 leaf/non-leaf segment 与 level-0
     *     稳定 root；LOB-capable 列只额外创建一个全表共享的 LOB segment，所有页修改共同形成一个物理原子单元。</li>
     *     <li>提交 MTR，将同批 redo 的结束 LSN 写入被修改页并释放 fix/latch；物理初始化或 commit 调用抛错时，
     *     只回滚仍处于 ACTIVE 的 MTR，并尽力 discard、失效、关闭和删除未完成 tablespace，清理失败作为
     *     suppressed exception 保留。</li>
     *     <li>对 commit LSN 执行 {@code flushThrough}，先满足 WAL，再刷出受该 LSN 约束的脏页并推进 checkpoint，
     *     随后 force 目标 tablespace。只有屏障全部成功才返回不可变 binding；若提交后的持久化阶段失败，则保留物理文件
     *     供 crash recovery 或受控 orphan cleanup 判定，不能把已经进入 redo 历史的建表动作当作未提交 MTR 直接删除。</li>
     * </ol>
     *
     * <p>与 MySQL/InnoDB 的差异：当前独立 DDL log 由 DD catalog sidecar 承载，SDI 是固定 page3
     * 单页快照而非内部 SDI B+Tree；物理完成与字典发布之间仍由 operation marker、committed DD 与受控路径共同裁决。</p>
     *
     * @param definition 已由 DD 分配稳定 table/space/index identity 的物理建表请求；不能为 {@code null}
     * @return 包含 tablespace、各索引 root/segment 以及可选 LOB segment 的稳定物理绑定
     * @throws DatabaseValidationException 请求为空、schema/key 无法映射或 redo 工作量计算溢出时抛出；这些失败不产生物理副作用
     */
    public TableStorageBinding createTable(StorageTableDefinition definition) {
        // 1. 先完成所有无副作用的 schema/key 可实现性校验；任何失败都必须早于文件创建和 MTR 页修改。
        if (definition == null) {
            throw new DatabaseValidationException("storage table definition must not be null");
        }
        for (StorageIndexDefinition index : definition.indexes()) {
            schemaMapper.tableSchema(definition, index.clustered());
            schemaMapper.indexKey(definition, index);
        }
        boolean requiresLobSegment = definition.columns().stream()
                .anyMatch(column -> isLobCapable(column.type().typeId()));

        // 2. 以本次 DDL 可能产生的最坏页镜像量申请 redo admission；预算不可表达时禁止进入物理创建阶段。
        long pageImages;
        try {
            long indexImages = Math.multiplyExact(6L, definition.indexes().size());
            // 一个额外 segment 最坏修改 page0/page2；再保留一个重复物理 delta 等价量，禁止低估 admission。
            // 固定 SDI page3 的 PAGE_INIT/body 与 page0 root 再保留 2 个完整页等价量。
            pageImages = Math.addExact(Math.addExact(14L, indexImages), requiresLobSegment ? 3L : 0L);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("DDL create redo workload overflows", overflow);
        }
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_TABLE_CREATE,
                RedoBudgetWorkload.pageImages(pageImages)));
        List<IndexStorageBinding> indexes = new ArrayList<>(definition.indexes().size());
        Optional<SegmentRef> lobSegment = Optional.empty();
        Lsn commitLsn;
        try {
            // 3. 所有 tablespace/FSP/segment/root 初始化都归属同一个 MTR，避免向 DD 暴露部分建成的索引集合。
            disk.createTablespace(mtr, definition.spaceId(), definition.path(), definition.initialSizeInPages(),
                    TablespaceType.GENERAL);
            for (StorageIndexDefinition index : definition.indexes()) {
                SegmentRef leaf = disk.createSegment(mtr, definition.spaceId(), SegmentPurpose.INDEX_LEAF);
                SegmentRef nonLeaf = disk.createSegment(mtr, definition.spaceId(), SegmentPurpose.INDEX_NON_LEAF);
                PageId root = disk.allocatePage(mtr, leaf);
                indexPages.createIndexPage(mtr, root, index.indexId(), 0);
                indexes.add(new IndexStorageBinding(index.indexId(), root, 0, leaf, nonLeaf));
            }
            if (requiresLobSegment) {
                lobSegment = Optional.of(disk.createSegment(mtr, definition.spaceId(), SegmentPurpose.LOB));
            }
            // 该 space 尚未向 DD 发布，外部业务 MTR 不可能取得其 page latch。此处已持有 index root 等高页号，
            // 唯一的 page3 逆序获取不会与并发线程形成环；作用域结束后立即恢复全序守卫。
            try (var ignored = mtr.allowOutOfOrderPageLatch(
                    "initialize reserved SDI page3 of an unpublished CREATE tablespace")) {
                sdiPages.initialize(mtr, definition.spaceId());
            }

            // 4. commit 为全部物理页分配统一结束 LSN 并释放 MTR 资源；正常返回前抛错则进入未完成建表补偿分支。
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackAndRemoveFailedCreate(mtr, definition, failure);
            throw failure;
        }

        // 5. 先让 redo、相关脏页与 checkpoint 覆盖 commit LSN，再 force 目标文件；屏障成功后 binding 才可供 DD 发布。
        flush.flushThrough(commitLsn, Duration.ofSeconds(30));
        store.force(definition.spaceId());
        log.info("created physical table: table={} space={} path={} indexes={} lobSegment={}", definition.tableId(),
                definition.spaceId().value(), definition.path(), indexes.size(), lobSegment.isPresent());
        return new TableStorageBinding(definition.tableId(), definition.spaceId(), definition.path(),
                definition.schemaVersion(), indexes, lobSegment);
    }

    /**
     * 将 DD 产生的 opaque SDI 快照写入 binding 指向的 GENERAL 表空间，并在返回前满足 WAL 与文件持久化。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>复核 binding、快照、timeout、table identity 和 opened path；失败时不开始 MTR。</li>
     *     <li>以专用 redo budget 开启 MTR，按 page0→page3 覆盖 SDI；legacy root=0 可由仓储原地补格式。</li>
     *     <li>提交 MTR；提交前异常只释放仍 ACTIVE 的资源，已提交后异常不得把 SDI 当成未写。</li>
     *     <li>flushThrough commit LSN 并 force tablespace，只有 redo 和 page3 均 durable 才返回。</li>
     * </ol>
     *
     * @param binding committed/待提交 DD 使用的稳定物理 binding，path 必须匹配已打开 space
     * @param information table/version 必须与 binding 对应的完整 opaque SDI
     * @param timeout redo/dirty durable 等待的正时限
     * @throws DatabaseValidationException 参数、identity 或 timeout 无效时抛出，不产生页副作用
     * @throws SerializedDictionaryInfoException root/envelope/容量或持久化失败时抛出，调用方不得发布未确认 DD
     */
    public void writeSerializedDictionaryInfo(TableStorageBinding binding,
                                              SerializedDictionaryInfo information,
                                              Duration timeout) {
        // 1. 所有纯校验必须发生在 begin 前，避免无效 identity 消耗 redo admission 或取得 tablespace lease。
        if (binding == null || information == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SDI write requires binding/information/positive timeout");
        }
        if (binding.tableId() != information.tableId()) {
            throw new DatabaseValidationException("SDI table identity does not match storage binding");
        }
        requireOpenedPath(binding, "SDI write");

        // 2. page repository 只接收数值 identity 与 opaque bytes，不形成对 DD 领域类型的依赖。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_SDI_WRITE));
        Lsn commitLsn;
        try {
            sdiPages.write(mtr, binding.spaceId(), information.tableId(),
                    information.dictionaryVersion(), information.payload());
            // 3. commit 统一盖 page LSN、发布 dirty 并释放 page0/page3 latch/fix。
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure("write SDI page failed: table=" + binding.tableId(), failure);
        }

        // 4. WAL gate 和 tablespace force 与物理 CREATE 使用相同持久边界。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(binding.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure("make SDI durable failed: table=" + binding.tableId(), failure);
        }
    }

    /**
     * 从 binding 指向的 GENERAL 表空间读取已校验 SDI；legacy root=0 或空 page3 返回 empty。
     *
     * @param binding 用于复核 table/space/path 的稳定物理 binding
     * @return CRC、envelope 与 identity 全部有效的快照，或尚未写入的 empty
     * @throws DatabaseValidationException binding 为空时抛出
     * @throws SerializedDictionaryInfoException path、页格式、CRC 或 table identity 不一致时抛出
     */
    public Optional<SerializedDictionaryInfo> readSerializedDictionaryInfo(TableStorageBinding binding) {
        if (binding == null) {
            throw new DatabaseValidationException("SDI read binding must not be null");
        }
        requireOpenedPath(binding, "SDI read");
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            Optional<SerializedDictionaryInfo> result = sdiPages.read(mtr, binding.spaceId())
                    .map(snapshot -> new SerializedDictionaryInfo(snapshot.tableId(),
                            snapshot.dictionaryVersion(), snapshot.payload()));
            if (result.isPresent() && result.orElseThrow().tableId() != binding.tableId()) {
                throw new SerializedDictionaryInfoException(
                        "SDI table identity does not match binding: expected=" + binding.tableId()
                                + " actual=" + result.orElseThrow().tableId());
            }
            mtrManager.commit(mtr);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure("read SDI page failed: table=" + binding.tableId(), failure);
        }
    }

    /**
     * 在既有表空间原子创建二级索引的两个 segment、稳定 root 与 page3 build descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 table/path、DDL identity 与非聚簇 index 定义，并用短只读 MTR 确认 footer 空闲；调用方此时
     *     必须已持 table MDL X，避免两条 DDL 竞争同一 footer。</li>
     *     <li>按 segment/FSP/root/page3 最坏页镜像申请 redo admission，再创建 leaf/non-leaf segment 与 root。</li>
     *     <li>在同一 MTR 写 build descriptor；窄 out-of-order scope 只覆盖高页号 root→固定 page3，
     *     table MDL X 和 purge/pin drain 必须保证不存在反向等待者。</li>
     *     <li>提交后执行 WAL/脏页/checkpoint 屏障并 force 表空间；只有 descriptor 与物理资源共同 durable 才返回。</li>
     * </ol>
     *
     * @param table 既有 ACTIVE table 的稳定物理绑定
     * @param ddlOperationId 已写 PREPARED marker 的正 DDL identity
     * @param dictionaryVersion CREATE INDEX 预留的目标字典版本
     * @param index 已完成列/key 映射能力验证的非聚簇索引定义
     * @param timeout redo/dirty/file durable 的正等待时限
     * @return 可用于 backfill、DD binding 与恢复交叉校验的 durable descriptor
     * @throws DatabaseValidationException 参数、index 形状或 identity 无效时抛出
     * @throws SerializedDictionaryInfoException page3/footer 损坏或已被另一条 build 占用时抛出
     */
    public SecondaryIndexBuildDescriptor beginSecondaryIndexBuild(
            TableStorageBinding table, long ddlOperationId, long dictionaryVersion,
            StorageIndexDefinition index, Duration timeout) {
        // 1. 所有纯校验和 footer preflight 早于 FSP 修改，失败不产生 segment/root 副作用。
        if (table == null || ddlOperationId <= 0 || dictionaryVersion <= 0 || index == null
                || index.clustered() || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "secondary index build requires table/positive identities/non-clustered index/timeout");
        }
        requireOpenedPath(table, "CREATE INDEX begin");
        if (readSecondaryIndexBuild(table).isPresent()) {
            throw new SerializedDictionaryInfoException(
                    "table already has a pending secondary index build: " + table.tableId());
        }

        // 2. 两个 segment、root、FSP metadata 与 footer 使用显式动态页镜像预算。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.DDL_TABLE_CREATE, RedoBudgetWorkload.pageImages(10)));
        Lsn commitLsn;
        SecondaryIndexBuildDescriptor result;
        try {
            SegmentRef leaf = disk.createSegment(mtr, table.spaceId(), SegmentPurpose.INDEX_LEAF);
            SegmentRef nonLeaf = disk.createSegment(mtr, table.spaceId(), SegmentPurpose.INDEX_NON_LEAF);
            PageId root = disk.allocatePage(mtr, leaf);
            indexPages.createIndexPage(mtr, root, index.indexId(), 0);
            IndexStorageBinding indexBinding =
                    new IndexStorageBinding(index.indexId(), root, 0, leaf, nonLeaf);
            result = new SecondaryIndexBuildDescriptor(
                    ddlOperationId, dictionaryVersion, table.tableId(), indexBinding);

            // 3. 调用方的 MDL X/purge/pin barrier 提供无环证明；scope 只覆盖本次固定 page3 逆序获取。
            try (var ignored = mtr.allowOutOfOrderPageLatch(
                    "CREATE INDEX stage: table MDL X and purge/pin drain exclude page3/FSP/index waiters")) {
                sdiPages.writeIndexBuild(mtr, table.spaceId(), toSdi(result));
            }
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure("stage secondary index build failed: table=" + table.tableId(), failure);
        }

        // 4. descriptor 是 crash recovery 所有权证据，必须与 segment/root 一起满足 WAL 并 force 后才能暴露。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
            return result;
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make secondary index build descriptor durable failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 读取当前 page3 的未决二级索引 build descriptor，不修改页或 DDL 状态。
     *
     * @param table 用于校验 space/path/table identity 的稳定 binding
     * @return 未决 descriptor，或 footer 为空
     * @throws SerializedDictionaryInfoException 页/footer 损坏或 table identity 不匹配时抛出
     */
    public Optional<SecondaryIndexBuildDescriptor> readSecondaryIndexBuild(TableStorageBinding table) {
        if (table == null) {
            throw new DatabaseValidationException("secondary index build read table must not be null");
        }
        requireOpenedPath(table, "CREATE INDEX descriptor read");
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            Optional<SecondaryIndexBuildDescriptor> result = sdiPages.readIndexBuild(mtr, table.spaceId())
                    .map(TableDdlStorageService::fromSdi);
            if (result.isPresent() && result.orElseThrow().tableId() != table.tableId()) {
                throw new SerializedDictionaryInfoException(
                        "secondary index build table identity mismatch: expected=" + table.tableId());
            }
            mtrManager.commit(mtr);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "read secondary index build descriptor failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 以 expected identity CAS 清空 durable build descriptor；该操作不释放已被 committed DD 引用的 segments。
     *
     * @param table 新旧 DD 都共享的表空间绑定
     * @param expected 调用方已与 marker/committed DD 交叉校验的 descriptor
     * @param timeout redo/dirty/file durable 的正时限
     * @throws SerializedDictionaryInfoException footer 已改变、损坏或持久化失败时抛出
     */
    public void clearSecondaryIndexBuild(TableStorageBinding table,
                                         SecondaryIndexBuildDescriptor expected, Duration timeout) {
        if (table == null || expected == null || timeout == null
                || timeout.isZero() || timeout.isNegative()
                || expected.tableId() != table.tableId()) {
            throw new DatabaseValidationException(
                    "secondary index build clear requires matching table/descriptor/positive timeout");
        }
        requireOpenedPath(table, "CREATE INDEX descriptor clear");
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_SDI_WRITE));
        Lsn commitLsn;
        try {
            sdiPages.clearIndexBuild(mtr, table.spaceId(), toSdi(expected));
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "clear secondary index build descriptor failed: table=" + table.tableId(), failure);
        }
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make secondary index build descriptor clear durable failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 扫描聚簇 live rows 并填充已 staged 的二级 B+Tree；不发布 DD，也不清 page3 descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验完整 storage definition、旧 binding 与 staged descriptor，并组装包含新 binding 的临时
     *     exact-version metadata；定义的 schema version 必须等于旧 binding 的 row format version。</li>
     *     <li>短只读 MTR 从聚簇最左 leaf 物化全部 live rows后释放所有 page latch/fix；v1 使用有界
     *     {@code Integer.MAX_VALUE} 内存列表，后续大型构建改 continuation batch。</li>
     *     <li>逐行投影紧凑 entry；UNIQUE 且 logical key 不含 NULL 时先扫描新树拒绝重复，再用独立写 MTR
     *     插入。每轮在 begin 前刷新 root level 并据此申请 split redo budget。</li>
     *     <li>最后刷新 root level，等待最后一批 redo/dirty/checkpoint 并 force 表空间，返回可写入 DD 的最终 binding。</li>
     * </ol>
     *
     * @param definition 包含旧聚簇/二级索引和本次新索引的完整物理 schema；schema version 是 row format version
     * @param existing 当前 committed DD 的旧物理 binding，不得已包含新 index id
     * @param staged begin 阶段 durable 的新 index root/segment descriptor
     * @param timeout 最终 WAL/dirty/file durable 的正时限
     * @return root level 已从物理页刷新、可追加进新 DD binding 的新 index binding
     * @throws SecondaryIndexBuildDuplicateKeyException UNIQUE backfill 发现重复非 NULL logical key 时抛出
     * @throws DatabaseValidationException metadata/identity/version 不一致时抛出且不扫描页
     */
    public IndexStorageBinding backfillSecondaryIndex(StorageTableDefinition definition,
                                                       TableStorageBinding existing,
                                                       SecondaryIndexBuildDescriptor staged,
                                                       Duration timeout) {
        // 1. 临时 aggregate 必须精确等于“旧 binding + staged binding”，禁止误把其它未提交 root 带入构建。
        if (definition == null || existing == null || staged == null || timeout == null
                || timeout.isZero() || timeout.isNegative()
                || definition.tableId() != existing.tableId()
                || definition.tableId() != staged.tableId()
                || definition.schemaVersion() != existing.rowFormatVersion()
                || existing.indexes().stream().anyMatch(
                        index -> index.indexId() == staged.indexBinding().indexId())) {
            throw new DatabaseValidationException("secondary backfill metadata/identity/version is invalid");
        }
        requireOpenedPath(existing, "CREATE INDEX backfill");
        List<IndexStorageBinding> bindings = new ArrayList<>(existing.indexes());
        bindings.add(staged.indexBinding());
        TableStorageBinding building = new TableStorageBinding(
                existing.tableId(), existing.spaceId(), existing.path(), existing.rowFormatVersion(),
                bindings, existing.lobSegment());
        var tableIndexes = indexMetadataFactory.createTable(definition, building);
        SecondaryIndexMetadata secondary =
                tableIndexes.requireSecondary(staged.indexBinding().indexId());

        // 2. 物化完成后提交只读 MTR，任何二级写入都不会同时持有聚簇 leaf latch。
        MiniTransaction scanMtr = mtrManager.beginReadOnly();
        List<LogicalRecord> rows;
        try {
            rows = btree.scanAll(scanMtr, tableIndexes.clusteredIndex(), Integer.MAX_VALUE).stream()
                    .map(result -> result.record()).toList();
            mtrManager.commit(scanMtr);
        } catch (RuntimeException failure) {
            rollbackIfBound(scanMtr, failure);
            throw failure;
        }

        // 3. table MDL X 排除了并发 DML；每行短 MTR 仍按真实 root level 独立预算并保持 page latch 生命周期。
        BTreeIndex current = secondary.index();
        Lsn lastCommit = null;
        for (LogicalRecord row : rows) {
            LogicalRecord entry = secondary.layout().toEntry(row, false);
            var logicalKey = secondary.layout().logicalKey(entry);
            boolean containsNull =
                    logicalKey.values().stream().anyMatch(ColumnValue.NullValue.class::isInstance);
            if (secondary.logicalUnique() && !containsNull) {
                MiniTransaction uniqueRead = mtrManager.beginReadOnly();
                try {
                    if (!btree.scanSecondaryPrefixIncludingDeleted(
                            uniqueRead, new SecondaryIndexMetadata(current, secondary.layout(), true),
                            logicalKey, 1).isEmpty()) {
                        throw new SecondaryIndexBuildDuplicateKeyException(
                                "CREATE UNIQUE INDEX found duplicate logical key: index="
                                        + current.indexId());
                    }
                    mtrManager.commit(uniqueRead);
                } catch (RuntimeException failure) {
                    rollbackIfBound(uniqueRead, failure);
                    throw failure;
                }
            }
            current = refreshRoot(current);
            MiniTransaction insert = mtrManager.begin(mtrManager.budgetFor(
                    RedoBudgetPurpose.SECONDARY_INDEX, BTreeRedoBudgetEstimator.insert(current.rootLevel())));
            try {
                BTreeInsertResult inserted = btree.insertSecondary(insert, current, entry);
                lastCommit = mtrManager.commit(insert);
                current = inserted.indexAfterInsert();
            } catch (RuntimeException failure) {
                rollbackIfBound(insert, failure);
                throw failure;
            }
        }

        // 4. 最终 root page header 是 binding level 的权威；最后一条插入的 LSN 覆盖全部先前提交。
        current = refreshRoot(current);
        if (lastCommit != null) {
            flush.flushThrough(lastCommit, timeout);
            store.force(existing.spaceId());
        }
        return new IndexStorageBinding(current.indexId(), current.rootPageId(), current.rootLevel(),
                current.leafSegment(), current.nonLeafSegment());
    }

    /**
     * 回收尚未被 committed DD 引用的 staged index segments，并在同一 MTR 清空 page3 descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table/descriptor/timeout，并读取当前 footer 做 exact identity CAS；不匹配时禁止释放任何 inode。</li>
     *     <li>分别用短只读 MTR 物化 leaf/non-leaf segment drop plan，释放全部 page latch 后计算动态 redo 上界。</li>
     *     <li>单个写 MTR 按 page0→page2→page3 顺序 drop 两个 segment并清 footer；失败时 descriptor 仍是恢复证据。</li>
     *     <li>提交后满足 WAL/dirty/checkpoint 并 force；只有物理资源和 footer 共同 durable 收敛才返回。</li>
     * </ol>
     *
     * @param table 未提交新 index 前仍由旧 DD 持有的表 binding
     * @param expected 与 DDL marker 精确匹配、确认不被 committed DD 引用的 build descriptor
     * @param timeout 最终持久化的正等待时限
     * @throws DatabaseValidationException 参数或 table identity 不一致时抛出
     * @throws SerializedDictionaryInfoException footer 改变、segment 损坏或持久化失败时抛出
     */
    public void rollbackSecondaryIndexBuild(TableStorageBinding table,
                                            SecondaryIndexBuildDescriptor expected,
                                            Duration timeout) {
        // 1. exact footer 是 segment 所有权证据；先读后比，不能根据调用方内存对象盲目 free inode。
        if (table == null || expected == null || timeout == null
                || timeout.isZero() || timeout.isNegative()
                || table.tableId() != expected.tableId()) {
            throw new DatabaseValidationException(
                    "secondary index rollback requires matching table/descriptor/positive timeout");
        }
        requireOpenedPath(table, "CREATE INDEX rollback");
        SecondaryIndexBuildDescriptor durable = readSecondaryIndexBuild(table).orElseThrow(() ->
                new SerializedDictionaryInfoException("secondary index build descriptor is absent"));
        if (!durable.equals(expected)) {
            throw new SerializedDictionaryInfoException(
                    "secondary index build descriptor changed before rollback");
        }

        // 2. drop plan 读取结束后才申请写预算；capacity 等待期间不持 page2 latch 或 FSP lease。
        SegmentDropPlan leafPlan = inspectDropPlan(expected.indexBinding().leafSegment());
        SegmentDropPlan nonLeafPlan = inspectDropPlan(expected.indexBinding().nonLeafSegment());
        RedoBudgetWorkload workload = indexDropWorkload(leafPlan, nonLeafPlan);

        // 3. 两个 inode 与 footer 构成同一恢复收敛单元；page3 晚于 page0/page2，保持管理页顺序。
        MiniTransaction drop = mtrManager.begin(
                mtrManager.budgetFor(RedoBudgetPurpose.DDL_INDEX_DROP, workload));
        Lsn commitLsn;
        try {
            disk.dropSegment(drop, expected.indexBinding().leafSegment());
            disk.dropSegment(drop, expected.indexBinding().nonLeafSegment());
            sdiPages.clearIndexBuild(drop, table.spaceId(), toSdi(expected));
            commitLsn = mtrManager.commit(drop);
        } catch (RuntimeException failure) {
            rollbackIfBound(drop, failure);
            throw translateSdiFailure(
                    "rollback secondary index build failed: table=" + table.tableId(), failure);
        }

        // 4. 返回即代表 inode free、page free intents 与 footer clear 都已通过同一 WAL/force 边界。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make secondary index build rollback durable failed: table=" + table.tableId(), failure);
        }
    }

    /** TEXT/BLOB/JSON 家族共用一个表级 LOB segment；VARCHAR/VARBINARY 仍只使用页内 variable 编码。 */
    private static boolean isLobCapable(StorageColumnTypeId typeId) {
        return switch (typeId) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
    }

    private static SdiIndexBuildDescriptor toSdi(SecondaryIndexBuildDescriptor descriptor) {
        return new SdiIndexBuildDescriptor(descriptor.ddlOperationId(), descriptor.dictionaryVersion(),
                descriptor.tableId(), descriptor.indexBinding());
    }

    /** 使用独立短读 MTR 刷新 root level，结束后调用方才能申请下一条结构写 redo budget。 */
    private BTreeIndex refreshRoot(BTreeIndex index) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            BTreeIndex refreshed = rootSnapshots.refresh(read, index);
            mtrManager.commit(read);
            return refreshed;
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
    }

    /** 在独立只读 MTR 中冻结 segment drop 规模，异常时不遗留 page2 latch。 */
    private SegmentDropPlan inspectDropPlan(SegmentRef segment) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            SegmentDropPlan plan = disk.inspectDropSegmentPlan(read, segment);
            mtrManager.commit(read);
            return plan;
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
    }

    /**
     * 保守估算两个 segment 的 free intent、inode/list/XDES 与 footer 写放大；checked arithmetic
     * 失败时禁止进入物理回收。
     */
    private static RedoBudgetWorkload indexDropWorkload(SegmentDropPlan leaf, SegmentDropPlan nonLeaf) {
        try {
            long fragments = Math.addExact(leaf.fragmentPageCount(), nonLeaf.fragmentPageCount());
            long extents = Math.addExact(leaf.extentCount(), nonLeaf.extentCount());
            long used = Math.addExact(leaf.usedPageCount(), nonLeaf.usedPageCount());
            long images = Math.addExact(12L, Math.multiplyExact(fragments, 4L));
            images = Math.addExact(images, Math.multiplyExact(extents, 6L));
            images = Math.addExact(images, Math.multiplyExact(used, 2L));
            return RedoBudgetWorkload.pageImages(images);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("secondary index drop redo workload overflows", overflow);
        }
    }

    private static SecondaryIndexBuildDescriptor fromSdi(SdiIndexBuildDescriptor descriptor) {
        return new SecondaryIndexBuildDescriptor(descriptor.ddlOperationId(), descriptor.dictionaryVersion(),
                descriptor.tableId(), descriptor.indexBinding());
    }

    /**
     * 删除已由 DD 标记不可见的物理表。独占 operation lease 阻止新 MTR 进入；drain 后写 DISCARDED marker 并
     * flushThrough，随后失效所有 frame、关闭 FileChannel 再删除。任何失败都保留 fail-closed 状态供恢复续作。
     */
    public void dropTable(TableStorageBinding binding, Duration timeout) {
        dropTable(binding, timeout, TableDdlStorageFaultInjector.NO_OP);
    }

    /**
     * 测试可见的故障注入版本。先复核 DD binding path 与已打开 space 的真实文件一致，
     * 再进入任何 marker/close/delete 副作；避免损坏 catalog 令引擎标记一个 space 却删另一条路径。
     */
    void dropTable(TableStorageBinding binding, Duration timeout, TableDdlStorageFaultInjector faultInjector) {
        if (binding == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("drop binding/positive timeout required");
        }
        if (faultInjector == null) {
            throw new DatabaseValidationException("drop fault injector must not be null");
        }
        requireOpenedPath(binding, "DDL DROP");
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            TablespaceDrainResult drained = flush.drainTablespace(binding.spaceId(), timeout);
            if (drained.timedOut()) {
                throw new TableDdlStorageException("timed out draining tablespace before drop: "
                        + binding.spaceId().value());
            }
            if (disk.tablespaceState(binding.spaceId()) != TablespaceState.DISCARDED) {
                MiniTransaction marker = mtrManager.begin(
                        mtrManager.budgetFor(RedoBudgetPurpose.DDL_TABLE_DROP));
                Lsn markerLsn;
                try {
                    disk.markTablespaceDiscarded(marker, binding.spaceId());
                    markerLsn = mtrManager.commit(marker);
                } catch (RuntimeException failure) {
                    rollbackIfBound(marker, failure);
                    throw failure;
                }
                flush.flushThrough(markerLsn, timeout);
                store.force(binding.spaceId());
                faultInjector.afterDiscardedDurable(binding);
            }
            pool.invalidateTablespace(binding.spaceId(), timeout);
            disk.closeTablespace(binding.spaceId());
            try {
                Files.deleteIfExists(binding.path());
            } catch (IOException e) {
                throw new TableDdlStorageException("delete discarded tablespace file failed: " + binding.path(), e);
            }
            log.info("dropped physical table: table={} space={} path={}", binding.tableId(),
                    binding.spaceId().value(), binding.path());
        }
    }

    private void rollbackAndRemoveFailedCreate(MiniTransaction mtr, StorageTableDefinition definition,
                                               RuntimeException failure) {
        rollbackIfBound(mtr, failure);
        try {
            disk.discardTablespace(definition.spaceId());
            pool.invalidateTablespace(definition.spaceId(), Duration.ofSeconds(5));
            disk.closeTablespace(definition.spaceId());
            Files.deleteIfExists(definition.path());
        } catch (RuntimeException | IOException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    /** 任何按 binding 读写物理页/文件的入口都先拒绝 catalog path 与真实句柄错绑。 */
    private void requireOpenedPath(TableStorageBinding binding, String operation) {
        Path openedPath = store.pathOf(binding.spaceId()).toAbsolutePath().normalize();
        Path bindingPath = binding.path().toAbsolutePath().normalize();
        if (!openedPath.equals(bindingPath)) {
            throw new TableDdlStorageException(operation + " binding path does not match opened tablespace: space="
                    + binding.spaceId().value() + " binding=" + bindingPath + " opened=" + openedPath);
        }
    }

    /** 保留稳定 API 异常，避免 DD/recovery 依赖 storage.sdi 内部异常类型。 */
    private static SerializedDictionaryInfoException translateSdiFailure(
            String message, RuntimeException failure) {
        if (failure instanceof SerializedDictionaryInfoException sdiFailure) {
            return sdiFailure;
        }
        return new SerializedDictionaryInfoException(message, failure);
    }

    /** commit 失败会由 manager 自行解绑；仅仍绑定时才做 uncommitted rollback。 */
    private void rollbackIfBound(MiniTransaction mtr, RuntimeException failure) {
        try {
            if (mtr.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(mtr);
            }
        } catch (RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
