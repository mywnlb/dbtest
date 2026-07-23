package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.SegmentDropPlan;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.io.TablespaceFileTransfer;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileInspection;
import cn.zhangyis.db.storage.api.lob.LobStorage;
import cn.zhangyis.db.storage.api.lob.LobFreeBatchPlan;
import cn.zhangyis.db.storage.api.lob.LobFreeTarget;
import cn.zhangyis.db.storage.api.lob.LobWriteAllocation;
import cn.zhangyis.db.storage.api.lob.LobWritePlan;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.TablespaceDrainResult;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.engine.StorageWriteAdmission;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.sdi.SdiPageRepository;
import cn.zhangyis.db.storage.sdi.SdiIndexBuildDescriptor;
import cn.zhangyis.db.storage.sdi.SdiIndexDdlAction;
import cn.zhangyis.db.storage.sdi.SdiIndexDdlDescriptor;
import cn.zhangyis.db.storage.sdi.SdiOnlineAlterAnchor;
import cn.zhangyis.db.storage.sdi.SdiOnlineAlterDescriptorAction;
import cn.zhangyis.db.storage.sdi.SdiOnlineAlterDescriptorEntry;
import cn.zhangyis.db.storage.sdi.SdiOnlineAlterDescriptorPage;
import cn.zhangyis.db.storage.sdi.SdiOnlineAlterDescriptorPageCodec;
import cn.zhangyis.db.storage.sdi.SdiOnlineAlterDescriptorPageRepository;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeDeleteResult;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeSecondaryRemovalResult;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.BTreeScanRange;
import cn.zhangyis.db.storage.btree.PreparedClusteredInsert;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.StorageKind;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferDdlBarrier;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * storage.api 的物理 DDL Facade。CREATE 负责 GENERAL tablespace/FSP/segment/index root，DROP 负责独占准入、
 * WAL-safe drain、持久 DISCARDED marker、Buffer Pool 失效、关闭句柄和删除文件；DD/MDL 状态由上层协调器拥有。
 */
@Slf4j
public final class TableDdlStorageService {

    /** 每个只读 MTR 最多物化的源聚簇行数；批次之间只保存完整 physical key continuation。 */
    private static final int REBUILD_SCAN_BATCH_SIZE = 256;

    /**
     * 本对象持有的 {@code mtrManager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final MiniTransactionManager mtrManager;
    /**
     * 本对象持有的 {@code disk} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DiskSpaceManager disk;
    /**
     * 本对象持有的 {@code indexPages} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final IndexPageAccess indexPages;
    /**
     * 本对象持有的 {@code pool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool pool;
    /**
     * 本对象持有的 {@code store} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PageStore store;
    /**
     * 本对象持有的 {@code flush} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final FlushService flush;
    /**
     * 本对象持有的 {@code accessController} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceAccessController accessController;
    /** 固定 page3 SDI 物理仓储；只处理 opaque payload 和页级完整性。 */
    private final SdiPageRepository sdiPages;
    /** 通用Online ALTER专用页仓储；只格式化已由FSP分配的descriptor页。 */
    private final SdiOnlineAlterDescriptorPageRepository onlineAlterDescriptorPages;
    /**
     * 本对象拥有的 {@code schemaMapper} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final StorageTableSchemaMapper schemaMapper = new StorageTableSchemaMapper();
    /** CREATE INDEX backfill 复用生产 B+Tree scan/insert，不另写页算法。 */
    private final SplitCapableBTreeIndexService btree;
    /** 每行写 MTR 前从稳定 root 页刷新层级，避免低估 split redo。 */
    private final BTreeRootSnapshotService rootSnapshots;
    /** shadow rebuild 跨空间读取旧 LOB 并在目标专属 segment 重分配，禁止复制旧 external reference。 */
    private final LobStorage lobStorage;
    /** 稳定 storage DTO/binding 到聚簇/二级 exact-version descriptor 的唯一工厂。 */
    private final BTreeIndexMetadataFactory indexMetadataFactory = new BTreeIndexMetadataFactory();
    /**
     * 本对象持有的 {@code fileTransfer} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceFileTransfer fileTransfer = new TablespaceFileTransfer();
    /**
     * 本对象持有的 {@code fileInspection} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceFileInspection fileInspection = new TablespaceFileInspection();
    /**
     * 本实例 tablespace 的固定页大小；外部文件检查必须使用该权威配置，不能相信 SQL 或文件名提供的值。
     */
    private final PageSize pageSize;
    /**
     * 统一写准入闸门；raw 文件动作没有 MTR 可替它检查，因此必须在获得表空间独占 lease 前显式校验。
     */
    private final StorageWriteAdmission writeAdmission;
    /** 可选 Change Buffer DDL 屏障；legacy 无 system.ibd 组合根为空。 */
    private final ChangeBufferDdlBarrier changeBufferBarrier;

    /**
     * 创建 {@code TableDdlStorageService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param mtrManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param disk 由组合根提供的 {@code DiskSpaceManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param indexPages 由组合根提供的 {@code IndexPageAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param store 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param flush 由组合根提供的 {@code FlushService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param accessController 由组合根提供的 {@code TablespaceAccessController} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param rootSnapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param lobStorage 由组合根提供的 LOB 读写门面；rebuild 必须用它迁移 external ownership
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public TableDdlStorageService(MiniTransactionManager mtrManager, DiskSpaceManager disk,
                                  IndexPageAccess indexPages, BufferPool pool, PageStore store,
                                  FlushService flush, TablespaceAccessController accessController,
                                  PageSize pageSize, SplitCapableBTreeIndexService btree,
                                  BTreeRootSnapshotService rootSnapshots,
                                  LobStorage lobStorage) {
        this(mtrManager, disk, indexPages, pool, store, flush, accessController, pageSize,
                btree, rootSnapshots, lobStorage, StorageWriteAdmission.normal(), null);
    }

    /**
     * 创建接入引擎统一写闸门的物理 DDL 门面。
     *
     * @param mtrManager 短物理事务协作者
     * @param disk 空间管理稳定门面
     * @param indexPages 索引页访问协作者
     * @param pool 缓冲池；离线动作依赖其 resident-page 证据
     * @param store 物理页文件入口
     * @param flush WAL-safe 刷盘协作者
     * @param accessController 与 MTR 共用的分空间准入控制器
     * @param pageSize 实例固定页大小
     * @param btree B+Tree DDL 协作者
     * @param rootSnapshots B+Tree root 稳定快照协作者
     * @param lobStorage LOB 迁移门面
     * @param writeAdmission 组合根唯一写准入闸门；不得为 {@code null}
     * @throws DatabaseValidationException 任一协作者为空时抛出
     */
    public TableDdlStorageService(MiniTransactionManager mtrManager, DiskSpaceManager disk,
                                  IndexPageAccess indexPages, BufferPool pool, PageStore store,
                                  FlushService flush, TablespaceAccessController accessController,
                                  PageSize pageSize, SplitCapableBTreeIndexService btree,
                                  BTreeRootSnapshotService rootSnapshots,
                                  LobStorage lobStorage,
                                  StorageWriteAdmission writeAdmission) {
        this(mtrManager, disk, indexPages, pool, store, flush, accessController, pageSize,
                btree, rootSnapshots, lobStorage, writeAdmission, null);
    }

    /**
     * 创建接入 Change Buffer 生命周期屏障的生产物理 DDL 门面；原构造器继续支持 legacy 无 system.ibd 实例。
     *
     * @param mtrManager 短物理事务协作者
     * @param disk 空间管理门面
     * @param indexPages 索引页访问入口
     * @param pool 共享 Buffer Pool
     * @param store 物理 PageStore
     * @param flush WAL-safe flush 服务
     * @param accessController 表空间 operation lease 控制器
     * @param pageSize 实例页大小
     * @param btree DDL B+Tree 协作者
     * @param rootSnapshots root level 快照服务
     * @param lobStorage LOB 迁移/释放服务
     * @param writeAdmission 引擎写准入闸门
     * @param changeBufferBarrier 可选 DROP 前全局 mutation 屏障
     */
    public TableDdlStorageService(MiniTransactionManager mtrManager, DiskSpaceManager disk,
                                  IndexPageAccess indexPages, BufferPool pool, PageStore store,
                                  FlushService flush, TablespaceAccessController accessController,
                                  PageSize pageSize, SplitCapableBTreeIndexService btree,
                                  BTreeRootSnapshotService rootSnapshots,
                                  LobStorage lobStorage,
                                  StorageWriteAdmission writeAdmission,
                                  ChangeBufferDdlBarrier changeBufferBarrier) {
        if (mtrManager == null || disk == null || indexPages == null || pool == null || store == null
                || flush == null || accessController == null || pageSize == null
                || btree == null || rootSnapshots == null || lobStorage == null || writeAdmission == null) {
            throw new DatabaseValidationException("table DDL storage collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.disk = disk;
        this.indexPages = indexPages;
        this.pool = pool;
        this.store = store;
        this.flush = flush;
        this.accessController = accessController;
        this.pageSize = pageSize;
        this.sdiPages = new SdiPageRepository(pool, pageSize);
        this.onlineAlterDescriptorPages =
                new SdiOnlineAlterDescriptorPageRepository(pool, pageSize);
        this.btree = btree;
        this.rootSnapshots = rootSnapshots;
        this.lobStorage = lobStorage;
        this.writeAdmission = writeAdmission;
        this.changeBufferBarrier = changeBufferBarrier;
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
        validateTableDefinition(definition);
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
     * 在不创建文件、不申请 redo、不打开 MTR 的前提下验证物理表定义可被当前 Record/B+Tree 实现解释。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空请求，保证后续 schema 映射不会产生非领域空指针异常。</li>
     *     <li>逐索引把完整表列映射为 leaf/node record schema，验证 type、charset/collation 与隐藏列布局。</li>
     *     <li>逐索引构造 key definition，验证 key part、prefix、顺序与列引用均可由比较器实现。</li>
     *     <li>全部映射成功后正常返回；本方法不修改 registry、文件、buffer page、redo 或 DDL 状态。</li>
     * </ol>
     *
     * @param definition 已由 DD 组装、准备进入物理 CREATE/rebuild 的完整定义；不得为 {@code null}
     * @throws DatabaseValidationException 定义为空或任一列/索引不能映射时抛出；失败保证没有物理副作用
     */
    public void validateTableDefinition(StorageTableDefinition definition) {
        // 1、空请求在进入 mapper 前用统一项目异常拒绝。
        if (definition == null) {
            throw new DatabaseValidationException(
                    "storage table definition must not be null");
        }
        for (StorageIndexDefinition index : definition.indexes()) {
            // 2、record schema 映射会校验当前 codec registry 支持的类型与字符排序规则。
            schemaMapper.tableSchema(definition, index.clustered());
            // 3、key 映射会校验 column identity、prefix 和 ASC/DESC 的可实现性。
            schemaMapper.indexKey(definition, index);
        }
        // 4、纯校验成功，不留下任何运行期或持久状态。
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换与受控 IO，把必要的 redo、dirty 或诊断副作用交给既有下游。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param binding 用于复核 table/space/path 的稳定物理 binding
     * @return CRC、envelope 与 identity 全部有效的快照，或尚未写入的 empty
     * @throws DatabaseValidationException binding 为空时抛出
     * @throws SerializedDictionaryInfoException path、页格式、CRC 或 table identity 不一致时抛出
     */
    public Optional<SerializedDictionaryInfo> readSerializedDictionaryInfo(TableStorageBinding binding) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (binding == null) {
            throw new DatabaseValidationException("SDI read binding must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        requireOpenedPath(binding, "SDI read");
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换与受控 IO，并维持领域不变量。
        MiniTransaction mtr = mtrManager.beginReadOnly();
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * 从已由恢复器按 exact identity 挂载、但尚未发布进 DD 的表空间读取 SDI envelope。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table/space/path，并要求当前打开句柄与 marker 路径完全一致，拒绝用同 space id 指向别的文件。</li>
     *     <li>在只读 MTR 中读取 page3 SDI，不解析 DD payload，也不构造未知的索引 binding。</li>
     *     <li>交叉验证 envelope table identity；成功提交只读 MTR，失败回滚 fix/latch 并保留原始 cause。</li>
     * </ol>
     *
     * @param tableId marker 与 manifest 共同确认的正表标识
     * @param spaceId 未发布 shadow 的正表空间标识
     * @param path marker manifest 中的规范绝对路径，必须与已打开句柄一致
     * @return 完成 envelope/CRC 校验的 SDI，尚未写入时为空
     * @throws DatabaseValidationException identity 或路径缺失时抛出，不取得页面资源
     * @throws SerializedDictionaryInfoException 路径错绑、SDI 损坏或 table identity 漂移时抛出，恢复必须 fail-closed
     */
    public Optional<SerializedDictionaryInfo> readSerializedDictionaryInfo(
            long tableId, SpaceId spaceId, Path path) {
        // 1、恢复只提供marker级identity，不能为了复用普通入口伪造index/segment binding。
        if (tableId <= 0 || spaceId == null || path == null) {
            throw new DatabaseValidationException(
                    "recovery SDI read requires table/space/path identity");
        }
        Path expected = path.toAbsolutePath().normalize();
        Path opened = store.pathOf(spaceId).toAbsolutePath().normalize();
        if (!opened.equals(expected)) {
            throw new SerializedDictionaryInfoException(
                    "recovery SDI path does not match opened tablespace: space="
                            + spaceId.value() + " expected=" + expected + " opened=" + opened);
        }

        // 2、storage层只解释envelope；完整TableDefinition仍由DD层codec负责。
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            Optional<SerializedDictionaryInfo> result = sdiPages.read(mtr, spaceId)
                    .map(snapshot -> new SerializedDictionaryInfo(snapshot.tableId(),
                            snapshot.dictionaryVersion(), snapshot.payload()));
            // 3、双写table identity防止路径正确但文件属于另一个表。
            if (result.isPresent() && result.orElseThrow().tableId() != tableId) {
                throw new SerializedDictionaryInfoException(
                        "recovery SDI table identity mismatch: expected=" + tableId
                                + " actual=" + result.orElseThrow().tableId());
            }
            mtrManager.commit(mtr);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "read recovery SDI page failed: table=" + tableId, failure);
        }
    }

    /**
     * 为一次通用INPLACE ALTER原子创建全部ADD root、引用全部DROP binding、descriptor chain和page3 anchor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证table/path、operation identity、manifest digest、action ordinal与ADD/DROP均属于当前source。</li>
     *     <li>预读page3确认无其它DDL owner，并按ADD资源与descriptor页数申请单个动态redo budget。</li>
     *     <li>同一MTR创建所有ADD leaf/non-leaf/root与一个DDL_DESCRIPTOR segment，分配并格式化完整page chain。</li>
     *     <li>最后在page3写ALT anchor，提交后等待WAL/dirty并force tablespace；只有整套owner durable才返回。</li>
     * </ol>
     *
     * @param table source committed aggregate的完整物理binding
     * @param ddlOperationId marker/journal/descriptor共用的正operation identity
     * @param targetDictionaryVersion 单次target aggregate的正字典版本
     * @param generation capture与descriptor共用的正代际
     * @param additions manifest中所有ADD INDEX请求；允许为空
     * @param drops manifest中所有DROP INDEX请求；允许为空
     * @param manifestDigest journal immutable manifest的32字节SHA-256
     * @param timeout redo/dirty/file force共用的正预算
     * @return durable descriptor set；entry严格按manifest ordinal排序
     * @throws DatabaseValidationException identity、action或source binding非法时抛出且不开始物理修改
     * @throws SerializedDictionaryInfoException footer占用、页/segment损坏或持久化失败时抛出
     */
    public OnlineAlterDescriptorSet beginOnlineAlterIndexDescriptors(
            TableStorageBinding table, long ddlOperationId, long targetDictionaryVersion,
            long generation, List<OnlineAlterIndexAddRequest> additions,
            List<OnlineAlterIndexDropRequest> drops, byte[] manifestDigest,
            Duration timeout) {
        // 1. 先完成全部纯identity验证，避免创建第一个segment后才发现重复ordinal或错误DROP owner。
        validateOnlineAlterDescriptorRequest(table, ddlOperationId, targetDictionaryVersion,
                generation, additions, drops, manifestDigest, timeout);
        requireOpenedPath(table, "online ALTER descriptor stage");
        if (readOnlineAlterDescriptorSet(table).isPresent()) {
            throw new SerializedDictionaryInfoException(
                    "table already has an online ALTER descriptor owner: " + table.tableId());
        }
        int descriptorCount = additions.size() + drops.size();
        int perPage = new SdiOnlineAlterDescriptorPageCodec(pageSize).maxEntriesPerPage();
        int descriptorPageCount = Math.floorDiv(descriptorCount + perPage - 1, perPage);

        // 2. 每个ADD最多触及两个inode/root/FSP镜像，descriptor segment/page/anchor另计；溢出在begin前拒绝。
        long pageImages;
        try {
            pageImages = Math.addExact(8L,
                    Math.addExact(Math.multiplyExact(10L, additions.size()),
                            Math.multiplyExact(3L, descriptorPageCount)));
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor redo workload overflows", overflow);
        }
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.DDL_TABLE_CREATE,
                RedoBudgetWorkload.pageImages(pageImages)));
        Lsn commitLsn;
        OnlineAlterDescriptorSet result;
        try {
            // 3. 所有物理资源先进入同一MTR，page3只有在完整chain可解释后才得到root指针。
            List<OnlineAlterIndexDescriptor> descriptors = new ArrayList<>(descriptorCount);
            for (OnlineAlterIndexAddRequest addition : additions) {
                StorageIndexDefinition definition = addition.definition();
                SegmentRef leaf = disk.createSegment(
                        mtr, table.spaceId(), SegmentPurpose.INDEX_LEAF);
                SegmentRef nonLeaf = disk.createSegment(
                        mtr, table.spaceId(), SegmentPurpose.INDEX_NON_LEAF);
                PageId root = disk.allocatePage(mtr, leaf);
                indexPages.createIndexPage(mtr, root, definition.indexId(), 0);
                descriptors.add(new OnlineAlterIndexDescriptor(
                        OnlineAlterIndexDescriptorAction.ADD, addition.actionOrdinal(),
                        new IndexStorageBinding(definition.indexId(), root, 0, leaf, nonLeaf)));
            }
            for (OnlineAlterIndexDropRequest drop : drops) {
                descriptors.add(new OnlineAlterIndexDescriptor(
                        OnlineAlterIndexDescriptorAction.DROP,
                        drop.actionOrdinal(), drop.binding()));
            }
            descriptors.sort(java.util.Comparator.comparingInt(
                    OnlineAlterIndexDescriptor::actionOrdinal));

            SegmentRef descriptorSegment = disk.createSegment(
                    mtr, table.spaceId(), SegmentPurpose.DDL_DESCRIPTOR);
            List<PageId> descriptorPages = new ArrayList<>(descriptorPageCount);
            for (int index = 0; index < descriptorPageCount; index++) {
                // 通用allocator只建立ALLOCATED envelope；专用仓储随后在同一MTR重写type/body。
                descriptorPages.add(disk.allocatePage(mtr, descriptorSegment));
            }
            for (int pageOrdinal = 0; pageOrdinal < descriptorPages.size(); pageOrdinal++) {
                int from = pageOrdinal * perPage;
                int to = Math.min(descriptors.size(), from + perPage);
                List<SdiOnlineAlterDescriptorEntry> entries = descriptors.subList(from, to)
                        .stream().map(TableDdlStorageService::toSdiOnlineAlterEntry).toList();
                long nextPageNo = pageOrdinal + 1 < descriptorPages.size()
                        ? descriptorPages.get(pageOrdinal + 1).pageNo().value() : 0L;
                onlineAlterDescriptorPages.formatAllocated(mtr,
                        descriptorPages.get(pageOrdinal),
                        new SdiOnlineAlterDescriptorPage(ddlOperationId,
                                targetDictionaryVersion, table.tableId(), generation,
                                descriptorSegment, pageOrdinal, nextPageNo, entries));
            }
            SdiOnlineAlterAnchor anchor = new SdiOnlineAlterAnchor(
                    ddlOperationId, targetDictionaryVersion, table.tableId(), generation,
                    descriptorPages.getFirst().pageNo().value(), descriptorCount,
                    manifestDigest);
            try (var ignored = mtr.allowOutOfOrderPageLatch(
                    "online ALTER stage owns table SU and writes page3 only after all new descriptor/index pages")) {
                sdiPages.writeOnlineAlterAnchor(mtr, table.spaceId(), anchor);
            }
            result = new OnlineAlterDescriptorSet(ddlOperationId,
                    targetDictionaryVersion, table.tableId(), generation,
                    descriptorSegment, descriptorPages, descriptors, manifestDigest);
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "stage online ALTER descriptor set failed: table=" + table.tableId(), failure);
        }

        // 4. descriptor是恢复删除权限，redo和数据文件未共同durable前不能写引用它的DDL marker阶段。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
            return result;
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make online ALTER descriptor set durable failed: table=" + table.tableId(),
                    failure);
        }
    }

    /**
     * 从page3 anchor有界遍历完整descriptor chain，交叉验证owner、ordinal、segment、entry数量和终止指针。
     *
     * @param table source或target committed aggregate提供的table/space/path identity
     * @return 完整descriptor set，或footer全零时为空
     * @throws SerializedDictionaryInfoException anchor/chain任一字段漂移或页损坏时抛出，恢复不得盲删资源
     */
    public Optional<OnlineAlterDescriptorSet> readOnlineAlterDescriptorSet(
            TableStorageBinding table) {
        if (table == null) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor read table must not be null");
        }
        requireOpenedPath(table, "online ALTER descriptor read");
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            Optional<SdiOnlineAlterAnchor> anchorOptional =
                    sdiPages.readOnlineAlterAnchor(mtr, table.spaceId());
            if (anchorOptional.isEmpty()) {
                mtrManager.commit(mtr);
                return Optional.empty();
            }
            SdiOnlineAlterAnchor anchor = anchorOptional.orElseThrow();
            if (anchor.tableId() != table.tableId() || anchor.descriptorCount() <= 0) {
                throw new SerializedDictionaryInfoException(
                        "online ALTER anchor table/count mismatch: table=" + table.tableId());
            }
            int perPage = new SdiOnlineAlterDescriptorPageCodec(pageSize).maxEntriesPerPage();
            int expectedPages = Math.floorDiv(anchor.descriptorCount() + perPage - 1, perPage);
            List<PageId> pages = new ArrayList<>(expectedPages);
            List<OnlineAlterIndexDescriptor> descriptors = new ArrayList<>(anchor.descriptorCount());
            java.util.Set<Long> visited = new java.util.HashSet<>();
            long pageNo = anchor.descriptorRootPageNo();
            SegmentRef descriptorSegment = null;
            for (int ordinal = 0; ordinal < expectedPages; ordinal++) {
                if (pageNo <= 0 || !visited.add(pageNo)) {
                    throw new SerializedDictionaryInfoException(
                            "online ALTER descriptor chain is short/cyclic: page=" + pageNo);
                }
                PageId pageId = PageId.of(table.spaceId(), PageNo.of(pageNo));
                SdiOnlineAlterDescriptorPage page = onlineAlterDescriptorPages.read(mtr, pageId);
                if (page.ddlOperationId() != anchor.ddlOperationId()
                        || page.targetDictionaryVersion() != anchor.targetDictionaryVersion()
                        || page.tableId() != anchor.tableId()
                        || page.generation() != anchor.generation()
                        || page.pageOrdinal() != ordinal
                        || descriptorSegment != null
                        && !descriptorSegment.equals(page.descriptorSegment())) {
                    throw new SerializedDictionaryInfoException(
                            "online ALTER descriptor page owner mismatch: page=" + pageId);
                }
                descriptorSegment = page.descriptorSegment();
                pages.add(pageId);
                page.entries().stream().map(TableDdlStorageService::fromSdiOnlineAlterEntry)
                        .forEach(descriptors::add);
                pageNo = page.nextPageNo();
            }
            if (pageNo != 0 || descriptors.size() != anchor.descriptorCount()
                    || descriptorSegment == null) {
                throw new SerializedDictionaryInfoException(
                        "online ALTER descriptor chain length/count mismatch");
            }
            OnlineAlterDescriptorSet result = new OnlineAlterDescriptorSet(
                    anchor.ddlOperationId(), anchor.targetDictionaryVersion(), anchor.tableId(),
                    anchor.generation(), descriptorSegment, pages, descriptors,
                    anchor.manifestDigest());
            mtrManager.commit(mtr);
            return Optional.of(result);
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "read online ALTER descriptor set failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * source DD仍为权威时回滚通用INPLACE：只释放ADD资源和descriptor segment，DROP资源保持可达。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>exact-read完整anchor/chain并与expected逐字段比较，错配时禁止释放任何segment。</li>
     *     <li>在独立短读MTR中冻结全部ADD与descriptor segment的drop plan，再申请动态redo预算。</li>
     *     <li>单写MTR释放ADD leaf/non-leaf与descriptor segment，最后exact-CAS清page3 anchor。</li>
     *     <li>提交后满足WAL并force；返回后旧DD的DROP binding仍完整有效。</li>
     * </ol>
     *
     * @param table 仍由source committed DD引用的物理aggregate
     * @param expected 与marker/journal交叉验证的descriptor set
     * @param timeout segment free、anchor clear和force共用的正预算
     */
    public void rollbackOnlineAlterIndexDescriptors(
            TableStorageBinding table, OnlineAlterDescriptorSet expected,
            Duration timeout) {
        // 1. descriptor是物理删除权限；完整owner不相等时保持所有资源供人工/恢复诊断。
        validateOnlineAlterDescriptorCleanup(table, expected, timeout, "rollback");
        OnlineAlterDescriptorSet durable = readOnlineAlterDescriptorSet(table).orElseThrow(() ->
                new SerializedDictionaryInfoException(
                        "online ALTER descriptor set is absent before rollback"));
        if (!sameOnlineAlterDescriptorSet(durable, expected)) {
            throw new SerializedDictionaryInfoException(
                    "online ALTER descriptor set changed before rollback");
        }

        // 2. redo admission等待前释放全部plan读取页；DROP binding不进入回滚释放集合。
        List<SegmentRef> segments = new ArrayList<>();
        for (OnlineAlterIndexDescriptor descriptor : expected.descriptors()) {
            if (descriptor.action() == OnlineAlterIndexDescriptorAction.ADD) {
                segments.add(descriptor.indexBinding().leafSegment());
                segments.add(descriptor.indexBinding().nonLeafSegment());
            }
        }
        segments.add(expected.descriptorSegment());
        RedoBudgetWorkload workload = onlineAlterDropWorkload(
                segments.stream().map(this::inspectDropPlan).toList());

        // 3. 页/segment free intents与anchor clear是一个物理收敛单元。
        MiniTransaction cleanup = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.DDL_INDEX_DROP, workload));
        Lsn commitLsn;
        try {
            for (SegmentRef segment : segments) {
                disk.dropSegment(cleanup, segment);
            }
            sdiPages.clearOnlineAlterAnchor(
                    cleanup, table.spaceId(), toSdiOnlineAlterAnchor(expected));
            commitLsn = mtrManager.commit(cleanup);
        } catch (RuntimeException failure) {
            rollbackIfBound(cleanup, failure);
            throw translateSdiFailure(
                    "rollback online ALTER descriptors failed: table=" + table.tableId(), failure);
        }

        // 4. WAL-safe force后才能让上层写ROLLED_BACK并删除journal。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make online ALTER descriptor rollback durable: table=" + table.tableId(),
                    failure);
        }
    }

    /**
     * target DD已提交且retirement barrier安全后前滚收敛：释放DROP资源与descriptor segment，ADD保持可达。
     *
     * @param table target committed aggregate；可已不包含被DROP的binding
     * @param expected 与marker/journal交叉验证的descriptor set
     * @param timeout 物理回收与force共用的正预算
     */
    public void finishOnlineAlterIndexDescriptors(
            TableStorageBinding table, OnlineAlterDescriptorSet expected,
            Duration timeout) {
        validateOnlineAlterDescriptorCleanup(table, expected, timeout, "finish");
        OnlineAlterDescriptorSet durable = readOnlineAlterDescriptorSet(table).orElseThrow(() ->
                new SerializedDictionaryInfoException(
                        "online ALTER descriptor set is absent before finish"));
        if (!sameOnlineAlterDescriptorSet(durable, expected)) {
            throw new SerializedDictionaryInfoException(
                    "online ALTER descriptor set changed before finish");
        }
        List<SegmentRef> segments = new ArrayList<>();
        for (OnlineAlterIndexDescriptor descriptor : expected.descriptors()) {
            if (descriptor.action() == OnlineAlterIndexDescriptorAction.DROP) {
                segments.add(descriptor.indexBinding().leafSegment());
                segments.add(descriptor.indexBinding().nonLeafSegment());
            }
        }
        segments.add(expected.descriptorSegment());
        RedoBudgetWorkload workload = onlineAlterDropWorkload(
                segments.stream().map(this::inspectDropPlan).toList());
        MiniTransaction cleanup = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.DDL_INDEX_DROP, workload));
        Lsn commitLsn;
        try {
            for (SegmentRef segment : segments) {
                disk.dropSegment(cleanup, segment);
            }
            sdiPages.clearOnlineAlterAnchor(
                    cleanup, table.spaceId(), toSdiOnlineAlterAnchor(expected));
            commitLsn = mtrManager.commit(cleanup);
        } catch (RuntimeException failure) {
            rollbackIfBound(cleanup, failure);
            throw translateSdiFailure(
                    "finish online ALTER descriptors failed: table=" + table.tableId(), failure);
        }
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make online ALTER descriptor finish durable: table=" + table.tableId(), failure);
        }
    }

    /**
     * 逐个ADD目标执行有界聚簇扫描并构建staged tree，最后一次性刷新descriptor chain中的root level。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>交叉校验source definition、descriptor owner与ADD请求一一对应，不读取DROP target。</li>
     *     <li>每个ADD复用成熟的256行continuation backfill；不同目标可重复扫描source但不持跨批页资源。</li>
     *     <li>汇总每棵树的最终root level，构造owner/segment/page不变的replacement set。</li>
     *     <li>单MTR对全部descriptor页执行exact-CAS replacement，WAL/force后返回durable set。</li>
     * </ol>
     *
     * @param sourceDefinition source committed表的完整storage schema
     * @param existing source committed binding
     * @param staged begin阶段durable的通用descriptor set
     * @param additions 与manifest ordinal/index identity一致的全部ADD定义
     * @param timeout 每棵backfill与最终descriptor force使用的正预算
     * @return ADD root level已刷新、其它owner字段不变的durable descriptor set
     */
    public OnlineAlterDescriptorSet backfillOnlineAlterIndexes(
            StorageTableDefinition sourceDefinition, TableStorageBinding existing,
            OnlineAlterDescriptorSet staged,
            List<OnlineAlterIndexAddRequest> additions, Duration timeout) {
        // 1. DROP-only operation无需扫描；有ADD时每个请求必须命中唯一descriptor。
        if (sourceDefinition == null || existing == null || staged == null
                || additions == null || timeout == null
                || timeout.isZero() || timeout.isNegative()
                || sourceDefinition.tableId() != existing.tableId()
                || staged.tableId() != existing.tableId()
                || sourceDefinition.schemaVersion() != existing.rowFormatVersion()) {
            throw new DatabaseValidationException(
                    "online ALTER backfill metadata/owner/timeout is invalid");
        }
        if (additions.isEmpty()) {
            return staged;
        }
        java.util.Map<Integer, OnlineAlterIndexDescriptor> byOrdinal =
                staged.descriptors().stream().collect(java.util.stream.Collectors.toMap(
                        OnlineAlterIndexDescriptor::actionOrdinal,
                        java.util.function.Function.identity()));
        List<OnlineAlterIndexDescriptor> replacements = new ArrayList<>(staged.descriptors());

        // 2. 单target临时definition严格等于source indexes + 当前ADD，避免把另一棵未发布root混入metadata。
        for (OnlineAlterIndexAddRequest addition : additions) {
            OnlineAlterIndexDescriptor descriptor = byOrdinal.get(addition.actionOrdinal());
            if (descriptor == null || descriptor.action() != OnlineAlterIndexDescriptorAction.ADD
                    || descriptor.indexBinding().indexId() != addition.definition().indexId()) {
                throw new DatabaseValidationException(
                        "online ALTER ADD request does not match durable descriptor");
            }
            List<StorageIndexDefinition> indexes = new ArrayList<>(sourceDefinition.indexes());
            indexes.add(addition.definition());
            StorageTableDefinition singleTarget = new StorageTableDefinition(
                    sourceDefinition.tableId(), sourceDefinition.spaceId(),
                    sourceDefinition.path(), sourceDefinition.schemaVersion(),
                    sourceDefinition.initialSizeInPages(), sourceDefinition.columns(), indexes);
            SecondaryIndexBuildDescriptor singleDescriptor = new SecondaryIndexBuildDescriptor(
                    staged.ddlOperationId(), staged.targetDictionaryVersion(), staged.tableId(),
                    descriptor.indexBinding());
            IndexStorageBinding finalBinding = backfillSecondaryIndexInternal(
                    singleTarget, existing, singleDescriptor, timeout, false);
            replacements.set(replacements.indexOf(descriptor),
                    new OnlineAlterIndexDescriptor(
                            OnlineAlterIndexDescriptorAction.ADD,
                            descriptor.actionOrdinal(), finalBinding));
        }

        // 3. replacement只改变ADD root level，descriptor segment/pages、DROP bindings与manifest digest完全不变。
        OnlineAlterDescriptorSet replacement = new OnlineAlterDescriptorSet(
                staged.ddlOperationId(), staged.targetDictionaryVersion(), staged.tableId(),
                staged.generation(), staged.descriptorSegment(), staged.descriptorPages(),
                replacements, staged.manifestDigest());

        // 4. 所有页exact-CAS成功才提交；崩溃在此之前仍可按旧level读取root页刷新或安全回滚segments。
        MiniTransaction update = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.DDL_TABLE_CREATE,
                RedoBudgetWorkload.pageImages(staged.descriptorPages().size() + 2L)));
        Lsn commitLsn;
        try {
            int perPage = new SdiOnlineAlterDescriptorPageCodec(pageSize).maxEntriesPerPage();
            for (int pageOrdinal = 0; pageOrdinal < staged.descriptorPages().size(); pageOrdinal++) {
                onlineAlterDescriptorPages.replace(update,
                        staged.descriptorPages().get(pageOrdinal),
                        descriptorPage(staged, pageOrdinal, perPage),
                        descriptorPage(replacement, pageOrdinal, perPage));
            }
            commitLsn = mtrManager.commit(update);
        } catch (RuntimeException failure) {
            rollbackIfBound(update, failure);
            throw translateSdiFailure(
                    "refresh online ALTER descriptor roots failed: table=" + existing.tableId(),
                    failure);
        }
        flush.flushThrough(commitLsn, timeout);
        store.force(existing.spaceId());
        return replacement;
    }

    /**
     * 通用INPLACE两遍reconciliation的删除阶段：按candidate完整physical entry幂等移除一个ADD target。
     * root shrink与descriptor chain root-level在同一MTR exact-CAS提交。
     *
     * @param sourceDefinition committed source storage schema
     * @param existing committed source binding
     * @param staged 当前durable descriptor set
     * @param addition manifest中与descriptor ordinal一致的ADD定义
     * @param entry candidate nested codec解码出的before或after完整secondary entry
     * @return 删除后root level已同步到descriptor chain的新set；ABSENT保持原set
     */
    public OnlineAlterDescriptorSet removeOnlineAlterIndexEntryExact(
            StorageTableDefinition sourceDefinition, TableStorageBinding existing,
            OnlineAlterDescriptorSet staged, OnlineAlterIndexAddRequest addition,
            LogicalRecord entry) {
        OnlineAlterBuildMetadata build = onlineAlterBuildMetadata(
                sourceDefinition, existing, staged, addition);
        SecondaryIndexMetadata metadata = build.secondary();
        BTreeIndex current = refreshRoot(metadata.index());
        SearchKey key = metadata.layout().physicalKey(entry);
        MiniTransaction read = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> found;
        try {
            found = btree.lookupIncludingDeleted(read, current, key);
            mtrManager.commit(read);
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
        if (found.isEmpty()) {
            return staged;
        }

        RedoBudgetWorkload workload = BTreeRedoBudgetEstimator.structuralDelete(
                        current.rootLevel())
                .plus(RedoBudgetWorkload.pageImages(staged.descriptorPages().size() + 1L));
        MiniTransaction remove = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.SECONDARY_INDEX, workload));
        try {
            BTreeSecondaryRemovalResult removed = found.orElseThrow().record().deleted()
                    ? btree.purgeDeleteMarkedSecondary(remove, current, key)
                    : btree.deletePublishedSecondary(remove, current, key);
            OnlineAlterDescriptorSet replacement = replaceOnlineAlterIndexBinding(
                    staged, addition.actionOrdinal(), indexBinding(removed.indexAfter()));
            try (var ignored = remove.allowOutOfOrderPageLatch(
                    "online ALTER reconciliation owns staged index before descriptor chain exact-CAS")) {
                replaceOnlineAlterDescriptorPages(remove, staged, replacement);
            }
            mtrManager.commit(remove);
            return replacement;
        } catch (RuntimeException failure) {
            rollbackIfBound(remove, failure);
            throw failure;
        }
    }

    /**
     * 通用INPLACE两遍reconciliation的ensure阶段：由candidate聚簇后缀回读source current truth，
     * DELETE保持target absent，live行执行UNIQUE裁决后插入或复活exact entry。
     *
     * @param sourceDefinition committed source storage schema
     * @param existing committed source binding
     * @param staged 当前durable descriptor set
     * @param addition manifest ADD目标
     * @param candidateEntry before/after任一完整entry，用其聚簇后缀定位source
     * @return current truth收敛后的descriptor set
     */
    public OnlineAlterDescriptorSet ensureOnlineAlterIndexCurrentForEntry(
            StorageTableDefinition sourceDefinition, TableStorageBinding existing,
            OnlineAlterDescriptorSet staged, OnlineAlterIndexAddRequest addition,
            LogicalRecord candidateEntry) {
        OnlineAlterBuildMetadata build = onlineAlterBuildMetadata(
                sourceDefinition, existing, staged, addition);
        SearchKey clusteredKey = build.secondary().layout().clusterKey(candidateEntry);
        MiniTransaction sourceRead = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> currentSource;
        try {
            currentSource = btree.lookup(sourceRead,
                    refreshRoot(build.clustered()), clusteredKey);
            mtrManager.commit(sourceRead);
        } catch (RuntimeException failure) {
            rollbackIfBound(sourceRead, failure);
            throw failure;
        }
        if (currentSource.isEmpty()) {
            return staged;
        }
        return ensureOnlineAlterIndexLive(sourceDefinition, existing, staged,
                addition, currentSource.orElseThrow().record());
    }

    /**
     * 对一个ADD target执行既有source↔target双向验证；验证不读取或修改legacy单slot footer。
     *
     * @param sourceDefinition committed source storage schema
     * @param existing committed binding
     * @param staged reconciliation后的descriptor set
     * @param addition 待验证ADD目标
     * @param batchSize 正有界扫描批次
     */
    public void verifyOnlineAlterIndex(
            StorageTableDefinition sourceDefinition, TableStorageBinding existing,
            OnlineAlterDescriptorSet staged, OnlineAlterIndexAddRequest addition,
            int batchSize) {
        OnlineAlterBuildMetadata build = onlineAlterBuildMetadata(
                sourceDefinition, existing, staged, addition);
        OnlineAlterIndexDescriptor descriptor = requireOnlineAlterAddDescriptor(
                staged, addition);
        verifySecondaryIndexBuild(build.definition(), existing,
                new SecondaryIndexBuildDescriptor(staged.ddlOperationId(),
                        staged.targetDictionaryVersion(), staged.tableId(),
                        descriptor.indexBinding()), batchSize);
    }

    /** 由一条稳定source row确保通用ADD target存在live entry，并把结构root变化原子写入descriptor chain。 */
    private OnlineAlterDescriptorSet ensureOnlineAlterIndexLive(
            StorageTableDefinition sourceDefinition, TableStorageBinding existing,
            OnlineAlterDescriptorSet staged, OnlineAlterIndexAddRequest addition,
            LogicalRecord clusteredRow) {
        OnlineAlterBuildMetadata build = onlineAlterBuildMetadata(
                sourceDefinition, existing, staged, addition);
        SecondaryIndexMetadata metadata = build.secondary();
        BTreeIndex current = refreshRoot(metadata.index());
        LogicalRecord entry = metadata.layout().toEntry(clusteredRow, false);
        SearchKey physicalKey = metadata.layout().physicalKey(entry);
        MiniTransaction read = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> found;
        try {
            found = btree.lookupIncludingDeleted(read, current, physicalKey);
            mtrManager.commit(read);
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
        if (found.isPresent() && !found.orElseThrow().record().deleted()) {
            return staged;
        }
        if (found.isEmpty() && metadata.logicalUnique()
                && metadata.layout().logicalKey(entry).values().stream()
                .noneMatch(ColumnValue.NullValue.class::isInstance)) {
            MiniTransaction uniqueRead = mtrManager.beginReadOnly();
            try {
                boolean duplicate = btree.scanSecondaryPrefixIncludingDeleted(
                                uniqueRead, new SecondaryIndexMetadata(
                                        current, metadata.layout(), true),
                                metadata.layout().logicalKey(entry), 2).stream()
                        .anyMatch(candidate -> !candidate.record().deleted());
                mtrManager.commit(uniqueRead);
                if (duplicate) {
                    throw new SecondaryIndexBuildDuplicateKeyException(
                            "online ALTER found duplicate UNIQUE key: index=" + current.indexId());
                }
            } catch (RuntimeException failure) {
                rollbackIfBound(uniqueRead, failure);
                throw failure;
            }
        }
        RedoBudgetWorkload workload = BTreeRedoBudgetEstimator.insert(current.rootLevel())
                .plus(RedoBudgetWorkload.pageImages(staged.descriptorPages().size() + 1L));
        MiniTransaction write = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.SECONDARY_INDEX, workload));
        try {
            BTreeIndex after;
            if (found.isPresent()) {
                btree.setSecondaryDeleteMark(write, current, physicalKey, false);
                after = current;
            } else {
                after = btree.insertSecondary(write, current, entry).indexAfterInsert();
            }
            OnlineAlterDescriptorSet replacement = replaceOnlineAlterIndexBinding(
                    staged, addition.actionOrdinal(), indexBinding(after));
            try (var ignored = write.allowOutOfOrderPageLatch(
                    "online ALTER ensure owns staged index before descriptor chain exact-CAS")) {
                replaceOnlineAlterDescriptorPages(write, staged, replacement);
            }
            mtrManager.commit(write);
            return replacement;
        } catch (RuntimeException failure) {
            rollbackIfBound(write, failure);
            throw failure;
        }
    }

    /** 构造source indexes + 单ADD的临时exact metadata，避免把其它未发布root或待DROP定义混入。 */
    private OnlineAlterBuildMetadata onlineAlterBuildMetadata(
            StorageTableDefinition sourceDefinition, TableStorageBinding existing,
            OnlineAlterDescriptorSet staged, OnlineAlterIndexAddRequest addition) {
        if (sourceDefinition == null || existing == null || staged == null || addition == null
                || sourceDefinition.tableId() != existing.tableId()
                || staged.tableId() != existing.tableId()
                || sourceDefinition.schemaVersion() != existing.rowFormatVersion()) {
            throw new DatabaseValidationException(
                    "online ALTER mutation metadata/owner is invalid");
        }
        OnlineAlterIndexDescriptor descriptor = requireOnlineAlterAddDescriptor(staged, addition);
        List<StorageIndexDefinition> definitions = new ArrayList<>(sourceDefinition.indexes());
        definitions.add(addition.definition());
        StorageTableDefinition definition = new StorageTableDefinition(
                sourceDefinition.tableId(), sourceDefinition.spaceId(), sourceDefinition.path(),
                sourceDefinition.schemaVersion(), sourceDefinition.initialSizeInPages(),
                sourceDefinition.columns(), definitions);
        List<IndexStorageBinding> bindings = new ArrayList<>(existing.indexes());
        bindings.add(descriptor.indexBinding());
        TableStorageBinding binding = new TableStorageBinding(
                existing.tableId(), existing.spaceId(), existing.path(),
                existing.rowFormatVersion(), bindings, existing.lobSegment());
        TableIndexMetadata metadata = indexMetadataFactory.createTable(definition, binding);
        return new OnlineAlterBuildMetadata(definition, metadata.clusteredIndex(),
                metadata.requireSecondary(addition.definition().indexId()));
    }

    /** 精确定位ADD descriptor并核对manifest ordinal/index identity。 */
    private static OnlineAlterIndexDescriptor requireOnlineAlterAddDescriptor(
            OnlineAlterDescriptorSet staged, OnlineAlterIndexAddRequest addition) {
        return staged.descriptors().stream().filter(descriptor ->
                        descriptor.actionOrdinal() == addition.actionOrdinal())
                .findFirst().filter(descriptor ->
                        descriptor.action() == OnlineAlterIndexDescriptorAction.ADD
                                && descriptor.indexBinding().indexId()
                                == addition.definition().indexId())
                .orElseThrow(() -> new DatabaseValidationException(
                        "online ALTER ADD request does not match descriptor owner"));
    }

    /** 只替换一个ADD descriptor的root binding，其余owner、顺序与manifest digest保持不变。 */
    private static OnlineAlterDescriptorSet replaceOnlineAlterIndexBinding(
            OnlineAlterDescriptorSet staged, int actionOrdinal,
            IndexStorageBinding binding) {
        List<OnlineAlterIndexDescriptor> descriptors = staged.descriptors().stream()
                .map(descriptor -> descriptor.actionOrdinal() == actionOrdinal
                        ? new OnlineAlterIndexDescriptor(descriptor.action(),
                        descriptor.actionOrdinal(), binding)
                        : descriptor).toList();
        return new OnlineAlterDescriptorSet(staged.ddlOperationId(),
                staged.targetDictionaryVersion(), staged.tableId(), staged.generation(),
                staged.descriptorSegment(), staged.descriptorPages(), descriptors,
                staged.manifestDigest());
    }

    /** 在调用方写MTR中对chain全部页执行exact-CAS；任一页漂移使整个MTR失败。 */
    private void replaceOnlineAlterDescriptorPages(
            MiniTransaction mtr, OnlineAlterDescriptorSet expected,
            OnlineAlterDescriptorSet replacement) {
        int perPage = new SdiOnlineAlterDescriptorPageCodec(pageSize).maxEntriesPerPage();
        for (int pageOrdinal = 0; pageOrdinal < expected.descriptorPages().size(); pageOrdinal++) {
            onlineAlterDescriptorPages.replace(mtr,
                    expected.descriptorPages().get(pageOrdinal),
                    descriptorPage(expected, pageOrdinal, perPage),
                    descriptorPage(replacement, pageOrdinal, perPage));
        }
    }

    /** 单ADD临时definition、source clustered与target secondary的不可变内部聚合。 */
    private record OnlineAlterBuildMetadata(StorageTableDefinition definition,
                                            BTreeIndex clustered,
                                            SecondaryIndexMetadata secondary) {
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param table 用于校验 space/path/table identity 的稳定 binding
     * @return 未决 descriptor，或 footer 为空
     * @throws SerializedDictionaryInfoException 页/footer 损坏或 table identity 不匹配时抛出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<SecondaryIndexBuildDescriptor> readSecondaryIndexBuild(TableStorageBinding table) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (table == null) {
            throw new DatabaseValidationException("secondary index build read table must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        requireOpenedPath(table, "CREATE INDEX descriptor read");
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        MiniTransaction mtr = mtrManager.beginReadOnly();
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * 在 DD 删除目标索引前，把其精确物理 binding 与 DDL identity 持久化到 page3。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table、DDL/version、timeout 与索引 binding；目标必须是当前 binding 中非首位的精确成员，
     *     避免物理层接受聚簇索引删除。</li>
     *     <li>用短只读 MTR 确认 footer 为空；调用方的 table MDL SU 排除另一个同表结构修改者，普通DML仍可继续。</li>
     *     <li>以 DDL_SDI_WRITE 预算写入带 DROP action 的 v2 descriptor，不修改 index page 或 segment inode。</li>
     *     <li>等待 footer redo/dirty 满足 WAL 并 force 表空间；只有恢复所有权 durable 后才允许上层提交新 DD。</li>
     * </ol>
     *
     * @param table 当前 committed DD 的完整物理表绑定
     * @param ddlOperationId 已写 PREPARED marker 的正 DDL identity
     * @param dictionaryVersion 删除索引后 table aggregate 的目标字典版本
     * @param index 当前 DD 中待删除二级索引的精确物理 binding
     * @param timeout footer WAL、dirty 与文件 force 的正等待时限
     * @return durable DROP descriptor；返回值可用于 DD 提交后物理回收与 crash recovery
     * @throws DatabaseValidationException 参数无效、目标不是当前二级索引或 identity 不一致时抛出
     * @throws SerializedDictionaryInfoException footer 已占用、页损坏或持久化失败时抛出
     */
    public SecondaryIndexDropDescriptor beginSecondaryIndexDrop(
            TableStorageBinding table, long ddlOperationId, long dictionaryVersion,
            IndexStorageBinding index, Duration timeout) {
        // 1. 物理层以 binding 顺序识别聚簇入口；精确 contains 防止调用方拼接相同 index id 的伪造 segment。
        if (table == null || ddlOperationId <= 0 || dictionaryVersion <= 0
                || index == null || timeout == null || timeout.isZero() || timeout.isNegative()
                || table.indexes().isEmpty() || table.indexes().getFirst().equals(index)
                || !table.indexes().contains(index)
                || !index.rootPageId().spaceId().equals(table.spaceId())) {
            throw new DatabaseValidationException(
                    "secondary index drop requires current non-clustered binding/positive identities/timeout");
        }
        requireOpenedPath(table, "DROP INDEX begin");

        // 2. 非空 BUILD footer 会以 action mismatch fail-closed，非空 DROP footer 则明确报告同表已有未决 DDL。
        if (readSecondaryIndexDrop(table).isPresent()) {
            throw new SerializedDictionaryInfoException(
                    "table already has a pending secondary index drop: " + table.tableId());
        }

        SecondaryIndexDropDescriptor result = new SecondaryIndexDropDescriptor(
                ddlOperationId, dictionaryVersion, table.tableId(), index);
        // 3. 本阶段只写 page3，不释放任何 DD 仍可达的物理资源。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_SDI_WRITE));
        Lsn commitLsn;
        try {
            sdiPages.writeIndexDrop(mtr, table.spaceId(), toSdi(result));
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "stage secondary index drop failed: table=" + table.tableId(), failure);
        }

        // 4. DD commit 必须晚于 descriptor durable；异常时 PREPARED marker 让恢复按旧 DD 清 footer。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
            return result;
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make secondary index drop descriptor durable failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 读取 page3 的未决 DROP INDEX descriptor，不修改页或 DDL 状态。
     *
     * @param table 用于校验 space/path/table identity 的当前或新 DD binding
     * @return 未决 DROP descriptor，或 footer 为空
     * @throws SerializedDictionaryInfoException footer 损坏、动作不是 DROP 或 table identity 不匹配时抛出
     * @throws DatabaseValidationException table 为空时抛出
     */
    public Optional<SecondaryIndexDropDescriptor> readSecondaryIndexDrop(TableStorageBinding table) {
        if (table == null) {
            throw new DatabaseValidationException("secondary index drop read table must not be null");
        }
        requireOpenedPath(table, "DROP INDEX descriptor read");
        MiniTransaction mtr = mtrManager.beginReadOnly();
        try {
            Optional<SecondaryIndexDropDescriptor> result = sdiPages.readIndexDrop(
                            mtr, table.spaceId())
                    .map(TableDdlStorageService::fromSdiDrop);
            if (result.isPresent() && result.orElseThrow().tableId() != table.tableId()) {
                throw new SerializedDictionaryInfoException(
                        "secondary index drop table identity mismatch: expected=" + table.tableId());
            }
            mtrManager.commit(mtr);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(mtr, failure);
            throw translateSdiFailure(
                    "read secondary index drop descriptor failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 在旧 DD 仍包含目标索引时回滚 DROP，只清除 descriptor，不释放任何 segment。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table/expected/timeout，并读取 footer 做 exact identity CAS；不匹配时禁止写页。</li>
     *     <li>以短写 MTR 清除 DROP descriptor；索引 root、leaf/non-leaf inode 与页面保持原样。</li>
     *     <li>提交 footer clear redo，不发布 DD/cache，也不触碰普通 B+Tree 可达性。</li>
     *     <li>等待 WAL/dirty 并 force；返回后旧 DD 可继续安全引用原索引。</li>
     * </ol>
     *
     * @param table 仍包含目标索引的旧 committed binding
     * @param expected 与 marker、旧 DD 精确匹配的 DROP descriptor
     * @param timeout footer clear 的正持久化时限
     * @throws DatabaseValidationException 参数或 table identity 不一致时抛出
     * @throws SerializedDictionaryInfoException footer 缺失、改变、损坏或持久化失败时抛出
     */
    public void rollbackSecondaryIndexDrop(TableStorageBinding table,
                                           SecondaryIndexDropDescriptor expected,
                                           Duration timeout) {
        // 1. 只有当前 page3 exact descriptor 才授予本恢复任务清理 footer 的权限。
        validateSecondaryIndexDropArguments(table, expected, timeout, "rollback");
        requireOpenedPath(table, "DROP INDEX rollback");
        SecondaryIndexDropDescriptor durable = readSecondaryIndexDrop(table).orElseThrow(() ->
                new SerializedDictionaryInfoException("secondary index drop descriptor is absent"));
        if (!durable.equals(expected)) {
            throw new SerializedDictionaryInfoException(
                    "secondary index drop descriptor changed before rollback");
        }

        // 2. 旧 DD 仍引用 segment，所以回滚绝不检查或修改 inode，仅 exact-CAS 清 page3。
        MiniTransaction clear = mtrManager.begin(
                mtrManager.budgetFor(RedoBudgetPurpose.DDL_SDI_WRITE));
        Lsn commitLsn;
        try {
            sdiPages.clearIndexDrop(clear, table.spaceId(), toSdi(expected));
            // 3. page3 clear 是本次 MTR 唯一持久副作用。
            commitLsn = mtrManager.commit(clear);
        } catch (RuntimeException failure) {
            rollbackIfBound(clear, failure);
            throw translateSdiFailure(
                    "rollback secondary index drop failed: table=" + table.tableId(), failure);
        }

        // 4. 只有 footer clear durable 后才向上层报告 ROLLED_BACK 可写。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make secondary index drop rollback durable failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 在新 DD 已不再包含目标索引后，原子回收两个 segment 并清除 DROP descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验并 exact-read footer；descriptor 是资源所有权证据，缺失或改变时禁止盲目释放 inode。</li>
     *     <li>分别在短只读 MTR 中冻结 leaf/non-leaf drop plan，释放全部 page latch 后再等待写 redo admission。</li>
     *     <li>单个写 MTR 按管理页锁序 drop 两个 segment，随后 exact-CAS 清 page3；任一步失败都由 MTR 回滚，
     *     保留 descriptor 供重启重试。</li>
     *     <li>提交后等待 WAL/dirty/checkpoint 并 force；返回即代表 DD 不可达资源与恢复证据共同收敛。</li>
     * </ol>
     *
     * @param table 已发布新 DD 的表绑定；其中可以不再包含目标索引
     * @param expected 与 marker、旧 binding 精确匹配的 DROP descriptor
     * @param timeout segment free、footer clear 与文件 force 的正等待时限
     * @throws DatabaseValidationException 参数或 table identity 不一致时抛出
     * @throws SerializedDictionaryInfoException descriptor/segment 损坏、identity 改变或持久化失败时抛出
     */
    public void finishSecondaryIndexDrop(TableStorageBinding table,
                                         SecondaryIndexDropDescriptor expected,
                                         Duration timeout) {
        // 1. 上层必须先提交新 DD；物理层仍以 exact footer 阻止错误任务释放复用后的 inode。
        validateSecondaryIndexDropArguments(table, expected, timeout, "finish");
        requireOpenedPath(table, "DROP INDEX finish");
        SecondaryIndexDropDescriptor durable = readSecondaryIndexDrop(table).orElseThrow(() ->
                new SerializedDictionaryInfoException("secondary index drop descriptor is absent"));
        if (!durable.equals(expected)) {
            throw new SerializedDictionaryInfoException(
                    "secondary index drop descriptor changed before finish");
        }

        // 新 DD 已阻断目标索引写；必须先移除全局 mutation identity，随后 inode 才允许被复用。
        if (changeBufferBarrier != null) {
            changeBufferBarrier.discardIndex(
                    table.tableId(), expected.indexBinding().indexId(), timeout);
        }

        // 2. admission 等待前结束 plan MTR，避免持 page2 latch 等 redo capacity。
        SegmentDropPlan leafPlan = inspectDropPlan(expected.indexBinding().leafSegment());
        SegmentDropPlan nonLeafPlan = inspectDropPlan(expected.indexBinding().nonLeafSegment());
        RedoBudgetWorkload workload = indexDropWorkload(leafPlan, nonLeafPlan);

        // 3. 两个 inode 与 footer 属于同一物理收敛单元；MTR 失败不会留下 descriptor 已清但 segment 尚存。
        MiniTransaction drop = mtrManager.begin(
                mtrManager.budgetFor(RedoBudgetPurpose.DDL_INDEX_DROP, workload));
        Lsn commitLsn;
        try {
            disk.dropSegment(drop, expected.indexBinding().leafSegment());
            disk.dropSegment(drop, expected.indexBinding().nonLeafSegment());
            sdiPages.clearIndexDrop(drop, table.spaceId(), toSdi(expected));
            commitLsn = mtrManager.commit(drop);
        } catch (RuntimeException failure) {
            rollbackIfBound(drop, failure);
            throw translateSdiFailure(
                    "finish secondary index drop failed: table=" + table.tableId(), failure);
        }

        // 4. WAL-safe force 之后才能让 DDL log 进入 terminal COMMITTED。
        try {
            flush.flushThrough(commitLsn, timeout);
            store.force(table.spaceId());
        } catch (RuntimeException failure) {
            throw translateSdiFailure(
                    "make secondary index drop finish durable failed: table=" + table.tableId(), failure);
        }
    }

    /**
     * 以 exclusive continuation 扫描 staged secondary build 的一批聚簇 live rows。返回前提交只读 MTR，
     * 因此结果不携带页资源，可在批次之间执行 row-log 检查、取消或二级写 MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>交叉校验 definition、committed binding 与 staged descriptor，拒绝错误 owner 或已发布索引。</li>
     *     <li>用“旧 binding + staged binding”构造 exact-version 临时 metadata，只读取其中聚簇 descriptor。</li>
     *     <li>在短只读 MTR 内按 unbounded/after range 物化至多 limit 行，并在返回前提交释放全部 page guard。</li>
     *     <li>从最后一行提取完整聚簇物理键作为 continuation；不足 limit 的批次标记 complete。</li>
     * </ol>
     *
     * @param definition 包含本次新 secondary 定义的完整目标 storage schema
     * @param existing 当前 committed 表物理 binding，不得已含 staged index
     * @param staged page3 durable 的 build owner/root/segment descriptor
     * @param continuation 上批最后完整聚簇键；首批为空
     * @param limit 本批最大 live row 数，必须为正；生产默认使用 OnlineDdlConfig.scanBatchRows
     * @return 完全物化且不持页资源的批次、exclusive continuation 与尾批次标志
     * @throws DatabaseValidationException identity/version/continuation/limit 不合法时抛出
     */
    public OnlineIndexScanBatch scanSecondaryIndexBuildBatch(
            StorageTableDefinition definition,
            TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged,
            Optional<SearchKey> continuation,
            int limit) {
        // 1. 所有 identity/版本校验早于 MTR 与页访问，失败不推进 scan 或 staged tree。
        if (definition == null || existing == null || staged == null || continuation == null
                || limit <= 0 || definition.tableId() != existing.tableId()
                || definition.tableId() != staged.tableId()
                || definition.schemaVersion() != existing.rowFormatVersion()
                || existing.indexes().stream().anyMatch(
                index -> index.indexId() == staged.indexBinding().indexId())) {
            throw new DatabaseValidationException("online secondary scan metadata/identity/limit is invalid");
        }
        requireOpenedPath(existing, "online CREATE INDEX scan");

        // 2. 临时 aggregate 只加入当前 staged owner，不把其它未发布 root 当作 source metadata。
        List<IndexStorageBinding> bindings = new ArrayList<>(existing.indexes());
        bindings.add(staged.indexBinding());
        TableStorageBinding building = new TableStorageBinding(
                existing.tableId(), existing.spaceId(), existing.path(), existing.rowFormatVersion(),
                bindings, existing.lobSegment());
        BTreeIndex clustered = indexMetadataFactory.createTable(definition, building).clusteredIndex();

        // 3. scan 结果已物化；提交 read-only MTR 后调用方才获得列表，禁止跨批次保留 page latch/fix。
        MiniTransaction scan = mtrManager.beginReadOnly();
        List<cn.zhangyis.db.storage.btree.BTreeLookupResult> results;
        try {
            BTreeScanRange range = continuation
                    .map(key -> BTreeScanRange.after(key, limit))
                    .orElseGet(() -> BTreeScanRange.unbounded(limit));
            results = btree.scan(scan, clustered, range);
            mtrManager.commit(scan);
        } catch (RuntimeException failure) {
            rollbackIfBound(scan, failure);
            throw failure;
        }

        // 4. 完整聚簇键唯一且 exclusive lower，因此下一批不会重复或遗漏上一批最后一行。
        List<LogicalRecord> rows = results.stream().map(result -> result.record()).toList();
        Optional<SearchKey> next = rows.isEmpty() ? continuation
                : Optional.of(physicalKey(rows.getLast(), clustered));
        return new OnlineIndexScanBatch(rows, next, rows.size() < limit);
    }

    /**
     * 确保 staged tree 存在给定当前聚簇行对应的 live physical entry。ABSENT 插入、delete-marked 复活、
     * 已 live no-op 三种状态均幂等；结构变化后的 root binding 与 page3 descriptor 在同一 MTR 发布。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>重建 exact staged metadata、刷新 root level并把完整聚簇行投影为 secondary entry。</li>
     *     <li>短只读 MTR 检查完整 physical key；live 直接返回，marked 进入等长 revive 写路径。</li>
     *     <li>ABSENT 时对非 NULL logical UNIQUE 扫描 live 冲突，再执行一次 split-capable insert。</li>
     *     <li>写 MTR 内同步更新 page3 build descriptor 后提交，返回新的 root level/segment owner 快照。</li>
     * </ol>
     *
     * @param definition 含 staged index 的完整目标 schema
     * @param existing 当前 committed binding
     * @param staged 当前 page3 owner/root binding
     * @param clusteredRow cutover current-read 或 base scan 得到的完整 live 聚簇行
     * @return 与实际 staged root level 一致的新 descriptor
     */
    public SecondaryIndexBuildDescriptor ensureSecondaryIndexLive(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, LogicalRecord clusteredRow) {
        return ensureSecondaryIndexLiveInternal(
                definition, existing, staged, clusteredRow, true);
    }

    /**
     * base scan 专用幂等写入口。扫描期间可能看到尚未提交、随后回滚的物理版本，因此这里只构造完整 physical
     * entry，不提前裁决 logical UNIQUE；sealed 后的 current-read reconciliation 与双向验证负责最终唯一性。
     *
     * @param definition 含 staged index 的完整目标 schema
     * @param existing 当前 committed binding
     * @param staged 当前 page3 owner/root binding
     * @param clusteredRow base scan 物化的聚簇物理 live row
     * @return 与实际 staged root level 一致的新 descriptor
     */
    public SecondaryIndexBuildDescriptor ensureSecondaryIndexLiveForBaseScan(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, LogicalRecord clusteredRow) {
        return ensureSecondaryIndexLiveInternal(
                definition, existing, staged, clusteredRow, false);
    }

    /** 共享 absent/live/deleted 幂等写实现；enforceUnique 只允许 cutover 或 legacy blocking 路径启用。 */
    private SecondaryIndexBuildDescriptor ensureSecondaryIndexLiveInternal(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, LogicalRecord clusteredRow,
            boolean enforceUnique) {
        // 1. helper 统一交叉校验 identity 并构造唯一 staged metadata，row 投影不访问页。
        SecondaryIndexMetadata metadata = secondaryBuildMetadata(definition, existing, staged);
        BTreeIndex current = refreshRoot(metadata.index());
        LogicalRecord entry = metadata.layout().toEntry(clusteredRow, false);
        SearchKey physicalKey = metadata.layout().physicalKey(entry);

        // 2. 先在短只读 MTR 分类 live/marked/absent，返回后不持 leaf guard。
        MiniTransaction read = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> found;
        try {
            found = btree.lookupIncludingDeleted(read, current, physicalKey);
            mtrManager.commit(read);
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
        if (found.isPresent() && !found.orElseThrow().record().deleted()) {
            return descriptorFor(staged, current);
        }

        // 3. 只有 ABSENT 才检查 logical UNIQUE；marked 同 identity 直接 revive，不与自身冲突。
        if (enforceUnique && found.isEmpty() && metadata.logicalUnique()
                && metadata.layout().logicalKey(entry).values().stream()
                .noneMatch(ColumnValue.NullValue.class::isInstance)) {
            MiniTransaction uniqueRead = mtrManager.beginReadOnly();
            try {
                boolean duplicate = btree.scanSecondaryPrefixIncludingDeleted(
                                uniqueRead, new SecondaryIndexMetadata(current, metadata.layout(), true),
                                metadata.layout().logicalKey(entry), 2).stream()
                        .anyMatch(candidate -> !candidate.record().deleted());
                if (duplicate) {
                    throw new SecondaryIndexBuildDuplicateKeyException(
                            "online CREATE UNIQUE INDEX found committed duplicate logical key: index="
                                    + current.indexId());
                }
                mtrManager.commit(uniqueRead);
            } catch (RuntimeException failure) {
                rollbackIfBound(uniqueRead, failure);
                throw failure;
            }
        }

        // 4. 结构写与 page3 descriptor 属于同一 MTR；crash 不会留下新 level 配旧 footer。
        MiniTransaction write = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.SECONDARY_INDEX, BTreeRedoBudgetEstimator.insert(current.rootLevel())));
        try {
            BTreeIndex after;
            if (found.isPresent()) {
                btree.setSecondaryDeleteMark(write, current, physicalKey, false);
                after = current;
            } else {
                after = btree.insertSecondary(write, current, entry).indexAfterInsert();
            }
            SecondaryIndexBuildDescriptor result = descriptorFor(staged, after);
            try (var ignored = write.allowOutOfOrderPageLatch(
                    "online CREATE INDEX mutation: table gate owns staged tree and permits index-to-page3 order")) {
                sdiPages.updateIndexBuild(write, existing.spaceId(), toSdi(staged), toSdi(result));
            }
            mtrManager.commit(write);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(write, failure);
            throw failure;
        }
    }

    /**
     * 精确移除一个 candidate physical entry，无论它当前为 live、delete-marked 还是 absent。删除可能触发
     * merge/root shrink，新的 root level 与 page3 descriptor 在同一 MTR 提交。
     *
     * @param definition 含 staged index 的目标 schema
     * @param existing committed binding
     * @param staged 当前 descriptor
     * @param entry candidate codec 解码出的完整 secondary physical entry
     * @return 删除后 descriptor；ABSENT 时保持结构 identity
     */
    public SecondaryIndexBuildDescriptor removeSecondaryIndexEntryExact(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, LogicalRecord entry) {
        SecondaryIndexMetadata metadata = secondaryBuildMetadata(definition, existing, staged);
        BTreeIndex current = refreshRoot(metadata.index());
        SearchKey key = metadata.layout().physicalKey(entry);

        MiniTransaction read = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> found;
        try {
            found = btree.lookupIncludingDeleted(read, current, key);
            mtrManager.commit(read);
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
        if (found.isEmpty()) {
            return descriptorFor(staged, current);
        }

        MiniTransaction remove = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.SECONDARY_INDEX,
                BTreeRedoBudgetEstimator.structuralDelete(current.rootLevel())));
        try {
            BTreeSecondaryRemovalResult removal = found.orElseThrow().record().deleted()
                    ? btree.purgeDeleteMarkedSecondary(remove, current, key)
                    : btree.deletePublishedSecondary(remove, current, key);
            SecondaryIndexBuildDescriptor result = descriptorFor(staged, removal.indexAfter());
            try (var ignored = remove.allowOutOfOrderPageLatch(
                    "online CREATE INDEX remove: sealed gate excludes staged writers and permits index-to-page3 order")) {
                sdiPages.updateIndexBuild(remove, existing.spaceId(), toSdi(staged), toSdi(result));
            }
            mtrManager.commit(remove);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(remove, failure);
            throw failure;
        }
    }

    /**
     * 有界扫描 staged tree 的 live 与 delete-marked 完整物理视图，供 final 双向验证发现 extra/deleted entry。
     *
     * @param definition 含 staged index 的目标 schema
     * @param existing committed binding
     * @param staged 当前 descriptor
     * @param continuation 上批最后完整 secondary physical key
     * @param limit 正批次上限
     * @return 不持页资源的 including-deleted entry 批次
     */
    public OnlineIndexEntryBatch scanSecondaryIndexBuildEntriesIncludingDeleted(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, Optional<SearchKey> continuation, int limit) {
        if (continuation == null || limit <= 0) {
            throw new DatabaseValidationException("online secondary entry scan continuation/limit is invalid");
        }
        SecondaryIndexMetadata metadata = secondaryBuildMetadata(definition, existing, staged);
        BTreeIndex current = refreshRoot(metadata.index());
        MiniTransaction scan = mtrManager.beginReadOnly();
        List<cn.zhangyis.db.storage.btree.BTreeLookupResult> results;
        try {
            BTreeScanRange range = continuation.map(key -> BTreeScanRange.after(key, limit))
                    .orElseGet(() -> BTreeScanRange.unbounded(limit));
            results = btree.scanIncludingDeleted(scan, current, range);
            mtrManager.commit(scan);
        } catch (RuntimeException failure) {
            rollbackIfBound(scan, failure);
            throw failure;
        }
        List<LogicalRecord> entries = results.stream().map(result -> result.record()).toList();
        Optional<SearchKey> next = entries.isEmpty() ? continuation
                : Optional.of(metadata.layout().physicalKey(entries.getLast()));
        return new OnlineIndexEntryBatch(entries, next, entries.size() < limit);
    }

    /**
     * 以 candidate entry 携带的完整聚簇后缀读取 cutover 当前行；行仍 live 时投影并确保 target live，行已删除
     * 时保持 target absent。PK UPDATE 不在 v1 SQL 范围，因此 before/after 任一 entry 都给出相同 cluster key。
     *
     * @param definition 含 staged index 的目标 schema
     * @param existing committed binding
     * @param staged 当前 descriptor
     * @param candidateEntry before 或 after physical entry
     * @return 按当前聚簇真相修正后的 descriptor
     */
    public SecondaryIndexBuildDescriptor ensureSecondaryIndexCurrentForEntry(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, LogicalRecord candidateEntry) {
        SecondaryIndexMetadata metadata = secondaryBuildMetadata(definition, existing, staged);
        SearchKey clusteredKey = metadata.layout().clusterKey(candidateEntry);
        BTreeIndex clustered = clusteredBuildIndex(definition, existing, staged);
        MiniTransaction read = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> current;
        try {
            current = btree.lookup(read, clustered, clusteredKey);
            mtrManager.commit(read);
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
        return current.isEmpty() ? staged
                : ensureSecondaryIndexLive(definition, existing, staged,
                current.orElseThrow().record());
    }

    /**
     * 双向验证 cutover source 与 staged target 完全等价：每个 live 聚簇行有唯一 live entry，target 不含
     * delete-marked/extra/wrong entry。方法只读、批次有界，失败不修改树或 descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按聚簇 continuation 分批扫描 source，把每行投影为完整 physical key 并在 staged tree 点查。</li>
     *     <li>反向分批扫描 staged tree（含 delete-marked），按聚簇后缀回读 source 并校验 entry 精确相等。</li>
     * </ol>
     *
     * @param definition 含 staged index 的目标 schema
     * @param existing committed binding
     * @param staged reconciliation 后 descriptor
     * @param batchSize 正扫描批次上限
     * @throws DatabaseRuntimeException 发现 missing、extra、deleted 或 wrong entry 时抛出
     */
    public void verifySecondaryIndexBuild(StorageTableDefinition definition,
                                          TableStorageBinding existing,
                                          SecondaryIndexBuildDescriptor staged,
                                          int batchSize) {
        if (batchSize <= 0) {
            throw new DatabaseValidationException("online secondary verify batch size must be positive");
        }
        SecondaryIndexMetadata metadata = secondaryBuildMetadata(definition, existing, staged);
        BTreeIndex target = refreshRoot(metadata.index());
        BTreeIndex clustered = clusteredBuildIndex(definition, existing, staged);

        // 1. source→target：逐个聚簇 live row 点查完整 physical key，缺失或 marked 立即拒绝发布。
        Optional<SearchKey> sourceContinuation = Optional.empty();
        while (true) {
            OnlineIndexScanBatch source = scanSecondaryIndexBuildBatch(
                    definition, existing, staged, sourceContinuation, batchSize);
            for (LogicalRecord row : source.rows()) {
                LogicalRecord expected = metadata.layout().toEntry(row, false);
                MiniTransaction read = mtrManager.beginReadOnly();
                try {
                    Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> actual =
                            btree.lookupIncludingDeleted(read, target, metadata.layout().physicalKey(expected));
                    mtrManager.commit(read);
                    if (actual.isEmpty() || actual.orElseThrow().record().deleted()
                            || !actual.orElseThrow().record().equals(expected)) {
                        throw new TableDdlStorageException(
                                "online secondary verification found missing/deleted/wrong target entry");
                    }
                } catch (RuntimeException failure) {
                    rollbackIfBound(read, failure);
                    throw failure;
                }
            }
            if (source.complete()) {
                break;
            }
            sourceContinuation = source.continuation();
        }

        // 2. target→source：including-deleted 扫描可发现多余或残留 marked entry，并按 cluster key 回读 source。
        Optional<SearchKey> targetContinuation = Optional.empty();
        while (true) {
            OnlineIndexEntryBatch entries = scanSecondaryIndexBuildEntriesIncludingDeleted(
                    definition, existing, staged, targetContinuation, batchSize);
            for (LogicalRecord entry : entries.entries()) {
                if (entry.deleted()) {
                    throw new TableDdlStorageException(
                            "online secondary verification found delete-marked target entry");
                }
                SearchKey logicalKey = metadata.layout().logicalKey(entry);
                if (metadata.logicalUnique() && logicalKey.values().stream()
                        .noneMatch(ColumnValue.NullValue.class::isInstance)) {
                    MiniTransaction uniqueRead = mtrManager.beginReadOnly();
                    try {
                        long matching = btree.scanSecondaryPrefixIncludingDeleted(
                                        uniqueRead, new SecondaryIndexMetadata(
                                                target, metadata.layout(), true), logicalKey, 2).stream()
                                .filter(candidate -> !candidate.record().deleted()).count();
                        mtrManager.commit(uniqueRead);
                        if (matching > 1) {
                            throw new SecondaryIndexBuildDuplicateKeyException(
                                    "online CREATE UNIQUE INDEX found committed duplicate logical key: index="
                                            + target.indexId());
                        }
                    } catch (RuntimeException failure) {
                        rollbackIfBound(uniqueRead, failure);
                        throw failure;
                    }
                }
                MiniTransaction read = mtrManager.beginReadOnly();
                try {
                    Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> row =
                            btree.lookup(read, clustered, metadata.layout().clusterKey(entry));
                    mtrManager.commit(read);
                    if (row.isEmpty() || !metadata.layout().toEntry(
                            row.orElseThrow().record(), false).equals(entry)) {
                        throw new TableDdlStorageException(
                                "online secondary verification found extra/wrong target entry");
                    }
                } catch (RuntimeException failure) {
                    rollbackIfBound(read, failure);
                    throw failure;
                }
            }
            if (entries.complete()) {
                break;
            }
            targetContinuation = entries.continuation();
        }
    }

    /**
     * final publish 前等待事务 terminal high-water 与全部 staged redo/dirty 页安全落盘，再 force tablespace。
     *
     * @param existing build 所属 committed tablespace binding
     * @param transactionHighWater gate 记录的最大 committed terminal redo LSN
     * @param timeout WAL/dirty/file force 的正等待上限
     */
    public void makeSecondaryIndexBuildDurable(TableStorageBinding existing,
                                                Lsn transactionHighWater,
                                                Duration timeout) {
        if (existing == null || transactionHighWater == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("online secondary durable barrier args are invalid");
        }
        Lsn current = mtrManager.redoLogManager().currentLsn();
        Lsn target = current.value() >= transactionHighWater.value() ? current : transactionHighWater;
        flush.flushThrough(target, timeout);
        store.force(existing.spaceId());
    }

    /**
     * 扫描聚簇 live rows 并填充已 staged 的二级 B+Tree；不发布 DD，也不清 page3 descriptor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验完整 storage definition、旧 binding 与 staged descriptor，并组装包含新 binding 的临时
     *     exact-version metadata；定义的 schema version 必须等于旧 binding 的 row format version。</li>
     *     <li>以 256 行 exclusive continuation 批次物化聚簇 live rows，每批提交只读 MTR 后才开始二级写。</li>
     *     <li>逐行投影紧凑 entry；UNIQUE 且 logical key 不含 NULL 时先扫描新树拒绝重复，再用独立写 MTR
     *     插入。每轮在 begin 前刷新 root level并据此申请 split redo budget。</li>
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
        return backfillSecondaryIndexInternal(
                definition, existing, staged, timeout, true);
    }

    /** 共享blocking/online backfill；online base copy延后logical UNIQUE裁决到sealed reconciliation。 */
    private IndexStorageBinding backfillSecondaryIndexInternal(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged, Duration timeout,
            boolean enforceUniqueDuringScan) {
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

        // 2、3. 每批 read MTR 已先提交；二级写从不同时持有聚簇 leaf latch，内存上界固定为 256 行。
        BTreeIndex current = secondary.index();
        Lsn lastCommit = null;
        Optional<SearchKey> continuation = Optional.empty();
        while (true) {
            OnlineIndexScanBatch batch = scanSecondaryIndexBuildBatch(
                    definition, existing, staged, continuation, REBUILD_SCAN_BATCH_SIZE);
            for (LogicalRecord row : batch.rows()) {
                LogicalRecord entry = secondary.layout().toEntry(row, false);
                var logicalKey = secondary.layout().logicalKey(entry);
                boolean containsNull =
                        logicalKey.values().stream().anyMatch(ColumnValue.NullValue.class::isInstance);
                if (enforceUniqueDuringScan && secondary.logicalUnique() && !containsNull) {
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
            if (batch.complete()) {
                break;
            }
            continuation = batch.continuation();
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
     * 在未发布的新 tablespace 中复制全部 live 聚簇行并重建所有二级索引。调用方必须持 table MDL X
     * 且已等待 purge barrier；本方法不修改源空间或 DD。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验源/目标 identity 与列投影，并创建全新目标空间、segments 和空 root。</li>
     *     <li>以 256 行 continuation 批次在短只读 MTR 中物化源 live rows，批次之间不保留 page latch/fix。</li>
     *     <li>逐行投影目标列；旧 external LOB 完整读取后在目标专属 segment 重分配，再以独立短 MTR
     *     插入聚簇与全部二级树；unique 冲突在 DD 发布前失败。</li>
     *     <li>刷新所有 root binding，等待最后 redo/dirty/checkpoint 并 force 新空间；失败携带 shadow binding
     *     返回给拥有 durable marker 的 DD coordinator，由它统一执行精确补偿。</li>
     * </ol>
     *
     * <p>教学实现差异：当前 rebuild 是单线程 blocking copy，没有 MySQL online DDL row log、并行扫描、
     * 外排排序或可暂停进度；但扫描内存有界，LOB ownership 不跨 tablespace 复用。</p>
     *
     * @param request 完整源/目标 schema、binding 与列投影
     * @param timeout 最终 WAL/force 与失败 cleanup 使用的正有界时间
     * @return 已包含最终 root level/page 的新物理 binding
     * @throws DatabaseValidationException identity、投影或 REQUIRED 新列与非空源表冲突时抛出
     * @throws TableRebuildException 基础 shadow CREATE 后，行投影、UNIQUE 校验、索引插入或 force 失败时抛出；
     *                               异常保留原始 cause 并携带必须回收的 shadow binding
     */
    public TableStorageBinding rebuildTable(
            StorageTableRebuildRequest request, Duration timeout) {
        return rebuildTableInternal(
                request, timeout, true, OnlineShadowCopyObserver.NO_OP);
    }

    /**
     * 在Online ALTER的CAPTURING阶段执行有界shadow copy。与阻塞rebuild共用物理投影和LOB ownership协议，
     * 但不在base copy阶段裁决logical UNIQUE；并发写产生的暂态旧像由sealed reconciliation删除，最终双向验证
     * 才对稳定source truth执行唯一性检查。
     *
     * @param request source/target exact schema、binding与列投影；source必须仍是committed aggregate
     * @param timeout shadow WAL与tablespace force的正有界时限
     * @return 已完成base copy并携带全部最新root level的未发布shadow binding
     * @throws DatabaseValidationException 请求、路径、schema或时限非法时抛出；调用方不得发布shadow
     * @throws TableRebuildException copy或force失败时抛出并携带精确shadow binding，调用方应按marker回收
     */
    public TableStorageBinding rebuildTableOnline(
            StorageTableRebuildRequest request, Duration timeout) {
        return rebuildTableOnline(
                request, timeout, OnlineShadowCopyObserver.NO_OP);
    }

    /**
     * 执行可观察、可取消的 Online shadow base copy。observer 只在完整批次释放所有 page/MTR 资源后调用，
     * 因此上层抛出取消异常不会把 latch/fix 带入 DDL rollback 或 MDL 协作。
     *
     * @param request source/target exact schema、binding 与列投影
     * @param timeout shadow WAL 与 tablespace force 的正有界时限
     * @param observer 批次终点观察者；不得为空，抛出的领域异常会包装进携带 shadow binding 的异常
     * @return 完成全部 base copy 后的未发布 shadow binding
     * @throws DatabaseValidationException 请求、时限或 observer 无效时抛出
     * @throws TableRebuildException copy、observer 取消或 force 失败时抛出并携带 shadow binding
     */
    public TableStorageBinding rebuildTableOnline(
            StorageTableRebuildRequest request, Duration timeout,
            OnlineShadowCopyObserver observer) {
        return rebuildTableInternal(request, timeout, false, observer);
    }

    /** 共享blocking/online copy实现；enforceUniqueDuringCopy只允许无并发source writer的blocking路径启用。 */
    private TableStorageBinding rebuildTableInternal(
            StorageTableRebuildRequest request, Duration timeout,
            boolean enforceUniqueDuringCopy,
            OnlineShadowCopyObserver observer) {
        // 1、所有无副作用校验早于目标 tablespace 创建。
        if (request == null || timeout == null || timeout.isZero() || timeout.isNegative()
                || observer == null) {
            throw new DatabaseValidationException(
                    "table rebuild requires request and positive timeout");
        }
        requireOpenedPath(request.sourceBinding(), "ALTER TABLE shadow rebuild source");
        TableStorageBinding target = createTable(request.targetDefinition());
        try {
            TableIndexMetadata sourceIndexes = indexMetadataFactory.createTable(
                    request.sourceDefinition(), request.sourceBinding());
            TableIndexMetadata targetIndexes =
                    indexMetadataFactory.createTable(request.targetDefinition(), target);
            BTreeIndex clustered = targetIndexes.clusteredIndex();
            List<SecondaryIndexMetadata> secondaries =
                    new ArrayList<>(targetIndexes.secondaryIndexes());
            Optional<SearchKey> continuation = Optional.empty();
            Lsn lastCommit = null;

            // 2、每批源扫描在独立只读 MTR 内完成，返回值不持 page guard；完整 physical key 排除式续扫。
            while (true) {
                MiniTransaction scan = mtrManager.beginReadOnly();
                List<cn.zhangyis.db.storage.btree.BTreeLookupResult> batch;
                try {
                    BTreeScanRange range = continuation
                            .map(key -> BTreeScanRange.after(
                                    key, REBUILD_SCAN_BATCH_SIZE))
                            .orElseGet(() -> BTreeScanRange.unbounded(
                                    REBUILD_SCAN_BATCH_SIZE));
                    batch = btree.scan(
                            scan, sourceIndexes.clusteredIndex(), range);
                    mtrManager.commit(scan);
                } catch (RuntimeException failure) {
                    rollbackIfBound(scan, failure);
                    throw failure;
                }
                if (batch.isEmpty()) {
                    break;
                }
                if (request.rewrites().stream().anyMatch(
                        rewrite -> rewrite.sourceOrdinal() < 0
                                && rewrite.defaultValue().isEmpty())) {
                    throw new DatabaseValidationException(
                            "ADD COLUMN without a default requires an empty source table");
                }

                // 3、每行先 hydrate/project，再以目标 LOB ownership 发布聚簇，最后逐棵维护二级树。
                for (var result : batch) {
                    LogicalRecord sourceRow = result.record();
                    RebuildProjection projection = projectRebuildRow(
                            sourceRow, request.rewrites(),
                            sourceIndexes.clusteredIndex(), request.sourceBinding(),
                            targetIndexes.clusteredIndex(), target);
                    clustered = refreshRoot(clustered);
                    RebuildClusteredInsert clusteredResult =
                            insertRebuildClusteredRow(
                                    clustered, sourceRow, projection,
                                    request.targetDefinition().schemaVersion());
                    clustered = clusteredResult.index();
                    LogicalRecord row = clusteredResult.row();
                    lastCommit = clusteredResult.lsn();

                    for (int i = 0; i < secondaries.size(); i++) {
                        SecondaryIndexMetadata secondary = secondaries.get(i);
                        BTreeIndex current = refreshRoot(secondary.index());
                        LogicalRecord entry = secondary.layout().toEntry(row, false);
                        var logicalKey = secondary.layout().logicalKey(entry);
                        boolean containsNull = logicalKey.values().stream()
                                .anyMatch(ColumnValue.NullValue.class::isInstance);
                        if (enforceUniqueDuringCopy
                                && secondary.logicalUnique() && !containsNull) {
                            MiniTransaction uniqueRead =
                                    mtrManager.beginReadOnly();
                            try {
                                if (!btree.scanSecondaryPrefixIncludingDeleted(
                                        uniqueRead,
                                        new SecondaryIndexMetadata(
                                                current, secondary.layout(),
                                                true),
                                        logicalKey, 1).isEmpty()) {
                                    throw new SecondaryIndexBuildDuplicateKeyException(
                                            "ALTER TABLE rebuild found duplicate UNIQUE key: index="
                                                    + current.indexId());
                                }
                                mtrManager.commit(uniqueRead);
                            } catch (RuntimeException failure) {
                                rollbackIfBound(uniqueRead, failure);
                                throw failure;
                            }
                        }
                        MiniTransaction secondaryInsert =
                                mtrManager.begin(mtrManager.budgetFor(
                                        RedoBudgetPurpose.SECONDARY_INDEX,
                                        BTreeRedoBudgetEstimator.insert(
                                                current.rootLevel())));
                        try {
                            BTreeInsertResult inserted =
                                    btree.insertSecondary(
                                            secondaryInsert, current, entry);
                            lastCommit =
                                    mtrManager.commit(secondaryInsert);
                            secondaries.set(i, new SecondaryIndexMetadata(
                                    inserted.indexAfterInsert(),
                                    secondary.layout(),
                                    secondary.logicalUnique()));
                        } catch (RuntimeException failure) {
                            rollbackIfBound(secondaryInsert, failure);
                            throw failure;
                        }
                    }
                }
                continuation = Optional.of(physicalKey(
                        batch.getLast().record(),
                        sourceIndexes.clusteredIndex()));
                // 批次read MTR与逐行target MTR均已提交；回调可安全等待控制面或抛出取消。
                observer.onBatchCompleted(batch.size(), continuation.orElseThrow());
                if (batch.size() < REBUILD_SCAN_BATCH_SIZE) {
                    break;
                }
            }

            // 4、root grow 后的 page/level 必须进入最终 DD binding；返回前 force 新空间。
            clustered = refreshRoot(clustered);
            List<IndexStorageBinding> bindings = new ArrayList<>();
            bindings.add(indexBinding(clustered));
            for (SecondaryIndexMetadata secondary : secondaries) {
                bindings.add(indexBinding(refreshRoot(secondary.index())));
            }
            // binding 的逻辑顺序必须按 target definition，而 metadata 聚合把二级按 id 排序。
            java.util.Map<Long, IndexStorageBinding> byId = bindings.stream().collect(
                    java.util.stream.Collectors.toMap(IndexStorageBinding::indexId, value -> value));
            List<IndexStorageBinding> ordered = request.targetDefinition().indexes().stream()
                    .map(index -> byId.get(index.indexId())).toList();
            target = new TableStorageBinding(
                    target.tableId(), target.spaceId(), target.path(),
                    target.rowFormatVersion(), ordered, target.lobSegment());
            if (lastCommit != null) {
                flush.flushThrough(lastCommit, timeout);
            }
            store.force(target.spaceId());
            return target;
        } catch (RuntimeException failure) {
            // durable DDL marker 归 DD 所有；把完整 binding 交回 coordinator，避免 storage 删除结果与
            // marker 终态彼此失联。若进程在异常交付前崩溃，启动恢复仍按 marker exact path 删除。
            throw new TableRebuildException(
                    "ALTER TABLE shadow rebuild failed: table=" + target.tableId()
                            + " space=" + target.spaceId().value(),
                    target, failure);
        }
    }

    /**
     * Shadow reconciliation 第一遍只删除 candidate identity 的旧像；不读取 source current truth，也不重插。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 source/target schema、exact binding 与聚簇 identity，并点查 shadow 当前旧像。</li>
     *     <li>若旧像存在，先幂等删除其派生的全部 secondary；重复 identity 观察 ABSENT 时直接成功。</li>
     *     <li>按隐藏列 ownership 删除聚簇旧像，并在同一短 MTR 释放该旧像拥有的 target LOB。</li>
     *     <li>刷新全部 root/level，按 target definition 顺序返回 binding；本阶段保证 identity 最终 absent。</li>
     * </ol>
     *
     * @param request 本次 shadow operation 冻结的 source/target schema 与列投影
     * @param shadowBinding 未发布 shadow 当前物理 binding
     * @param clusteredIdentity journal 解码出的稳定聚簇物理键
     * @return 删除后包含最新 root/level 的 shadow binding
     * @throws DatabaseValidationException identity、binding 或 schema 错配时抛出且不发布结果
     * @throws TableDdlStorageException ownership、LOB 或 B+Tree 删除无法收敛时抛出
     */
    public TableStorageBinding deleteOnlineShadowIdentity(
            StorageTableRebuildRequest request,
            TableStorageBinding shadowBinding,
            SearchKey clusteredIdentity) {
        // 1、只建立target元数据；delete pass故意不读取source，避免在第一遍重新引入current truth。
        validateOnlineShadow(request, shadowBinding, clusteredIdentity);
        TableIndexMetadata targetMetadata = indexMetadataFactory.createTable(
                request.targetDefinition(), shadowBinding);
        BTreeIndex targetClustered = refreshRoot(targetMetadata.clusteredIndex());
        List<SecondaryIndexMetadata> targetSecondaries = targetMetadata.secondaryIndexes().stream()
                .map(secondary -> new SecondaryIndexMetadata(
                        refreshRoot(secondary.index()), secondary.layout(), secondary.logicalUnique()))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        MiniTransaction shadowRead = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> oldShadow;
        try {
            oldShadow = btree.lookupIncludingDeleted(
                    shadowRead, targetClustered, clusteredIdentity);
            mtrManager.commit(shadowRead);
        } catch (RuntimeException failure) {
            rollbackIfBound(shadowRead, failure);
            throw failure;
        }
        if (oldShadow.isEmpty()) {
            return shadowBinding(request.targetDefinition(), shadowBinding,
                    targetClustered, targetSecondaries);
        }

        LogicalRecord oldRow = oldShadow.orElseThrow().record();
        // 2、secondary先删使其不再指向即将删除的聚簇行；ABSENT支持重复candidate与重试。
        for (int index = 0; index < targetSecondaries.size(); index++) {
            SecondaryIndexMetadata secondary = targetSecondaries.get(index);
            BTreeIndex after = removeShadowSecondaryIfPresent(secondary, oldRow);
            targetSecondaries.set(index, new SecondaryIndexMetadata(
                    after, secondary.layout(), secondary.logicalUnique()));
        }

        // 3、clustered ownership与target LOB释放属于同一MTR，不能留下记录已删但LOB仍归属它的状态。
        Optional<LobFreeBatchPlan> lobFree = planShadowLobFree(
                oldRow, targetClustered, shadowBinding);
        RedoBudgetWorkload deleteWorkload = BTreeRedoBudgetEstimator.structuralDelete(
                targetClustered.rootLevel());
        if (lobFree.isPresent()) {
            deleteWorkload = deleteWorkload.plus(lobFree.orElseThrow().workload());
        }
        MiniTransaction delete = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.CLUSTERED_DELETE, deleteWorkload));
        try {
            BTreeDeleteResult deleted = btree.deleteClustered(
                    delete, targetClustered, clusteredIdentity,
                    oldRow.hiddenColumns().dbTrxId(), oldRow.hiddenColumns().dbRollPtr());
            if (!deleted.removed()) {
                throw new TableDdlStorageException(
                        "online shadow clustered ownership changed during delete pass");
            }
            targetClustered = deleted.indexAfter();
            if (lobFree.isPresent()) {
                try (var ignored = delete.allowOutOfOrderPageLatch(
                        "online shadow delete pass owns clustered leaf before target LOB/FSP cleanup")) {
                    lobStorage.freePlannedBatch(delete, lobFree.orElseThrow());
                }
            }
            mtrManager.commit(delete);
        } catch (RuntimeException failure) {
            rollbackIfBound(delete, failure);
            throw failure;
        }
        // 4、root grow/shrink结果必须沿binding传给第二遍和最终DD。
        return shadowBinding(request.targetDefinition(), shadowBinding,
                targetClustered, targetSecondaries);
    }

    /**
     * Shadow reconciliation 第二遍从 source current truth 幂等确保 candidate identity；第一遍已保证旧像 absent。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 exact binding，并分别建立 source/current 与 target 索引元数据。</li>
     *     <li>current source 不存在时保持 shadow absent；存在时在 page guard 外完成 LOB hydrate/列投影。</li>
     *     <li>shadow 聚簇不存在则插入投影行；重复 candidate 已插入时复用该 current 行。</li>
     *     <li>逐个 secondary 先幂等清除再按 current 行插入并裁决 UNIQUE，刷新 binding 后返回。</li>
     * </ol>
     *
     * @param request 本次 shadow operation 的不可变 schema/transform
     * @param shadowBinding delete pass 后的当前 shadow binding
     * @param clusteredIdentity candidate 的稳定聚簇物理键
     * @return current truth 已存在或保持 absent 后的最新 binding
     * @throws DatabaseValidationException 参数或 binding 错配时抛出
     * @throws TableDdlStorageException current truth 无法唯一投影或索引/LOB 写入失败时抛出
     */
    public TableStorageBinding ensureOnlineShadowIdentityCurrent(
            StorageTableRebuildRequest request,
            TableStorageBinding shadowBinding,
            SearchKey clusteredIdentity) {
        // 1、final X下source current truth稳定；两个metadata对象只在短MTR间传不可变root身份。
        validateOnlineShadow(request, shadowBinding, clusteredIdentity);
        TableIndexMetadata sourceMetadata = indexMetadataFactory.createTable(
                request.sourceDefinition(), request.sourceBinding());
        TableIndexMetadata targetMetadata = indexMetadataFactory.createTable(
                request.targetDefinition(), shadowBinding);
        BTreeIndex sourceClustered = refreshRoot(sourceMetadata.clusteredIndex());
        BTreeIndex targetClustered = refreshRoot(targetMetadata.clusteredIndex());
        List<SecondaryIndexMetadata> targetSecondaries = targetMetadata.secondaryIndexes().stream()
                .map(secondary -> new SecondaryIndexMetadata(
                        refreshRoot(secondary.index()), secondary.layout(), secondary.logicalUnique()))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

        MiniTransaction sourceRead = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> currentSource;
        try {
            currentSource = btree.lookup(sourceRead, sourceClustered, clusteredIdentity);
            mtrManager.commit(sourceRead);
        } catch (RuntimeException failure) {
            rollbackIfBound(sourceRead, failure);
            throw failure;
        }
        // 2、DELETE truth保持第一遍建立的ABSENT；不会从undo或candidate payload复活记录。
        if (currentSource.isEmpty()) {
            return shadowBinding(request.targetDefinition(), shadowBinding,
                    targetClustered, targetSecondaries);
        }

        LogicalRecord targetRow;
        MiniTransaction targetRead = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> existing;
        try {
            existing = btree.lookupIncludingDeleted(
                    targetRead, targetClustered, clusteredIdentity);
            mtrManager.commit(targetRead);
        } catch (RuntimeException failure) {
            rollbackIfBound(targetRead, failure);
            throw failure;
        }
        if (existing.isPresent()) {
            // 3、只可能来自本ensure pass中的重复candidate；delete pass已在本轮开始时清除了历史旧像。
            targetRow = existing.orElseThrow().record();
        } else {
            LogicalRecord sourceRow = currentSource.orElseThrow().record();
            RebuildProjection projection = projectRebuildRow(
                    sourceRow, request.rewrites(), sourceClustered,
                    request.sourceBinding(), targetClustered, shadowBinding);
            RebuildClusteredInsert inserted = insertRebuildClusteredRow(
                    targetClustered, sourceRow, projection,
                    request.targetDefinition().schemaVersion());
            targetClustered = inserted.index();
            targetRow = inserted.row();
        }

        // 4、先删后插让重复candidate与“clustered已写、secondary部分失败”的重试都收敛到exact current entry。
        for (int index = 0; index < targetSecondaries.size(); index++) {
            SecondaryIndexMetadata secondary = targetSecondaries.get(index);
            BTreeIndex removed = removeShadowSecondaryIfPresent(secondary, targetRow);
            SecondaryIndexMetadata refreshed = new SecondaryIndexMetadata(
                    removed, secondary.layout(), secondary.logicalUnique());
            BTreeIndex inserted = insertShadowSecondary(refreshed, targetRow, true);
            targetSecondaries.set(index, new SecondaryIndexMetadata(
                    inserted, secondary.layout(), secondary.logicalUnique()));
        }
        return shadowBinding(request.targetDefinition(), shadowBinding,
                targetClustered, targetSecondaries);
    }

    /**
     * 以一个clustered identity把未发布shadow修正到source current truth。该入口只允许final X、source writer和
     * capture lease均排空后调用；它不自行取得事务锁，也不读取undo版本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按exact target binding点查shadow旧像；存在时先幂等移除由旧像派生的全部secondary。</li>
     *     <li>在同一短MTR中按隐藏列所有权物理删除旧聚簇记录并释放它拥有的target LOB，避免遗留孤儿页。</li>
     *     <li>从committed source聚簇树执行current read；不存在表示DELETE，存在则重新hydrate、transform并写shadow。</li>
     *     <li>为新shadow行重建全部target secondary，刷新root level并按target definition顺序返回新binding。</li>
     * </ol>
     *
     * @param request 本次shadow operation冻结的source/target schema与列投影
     * @param shadowBinding 未发布shadow当前root/segment binding；路径和space必须与target definition精确一致
     * @param clusteredIdentity change-log解码出的完整、稳定聚簇物理键
     * @return 本次删除/插入后携带全部最新root level的新shadow binding
     * @throws DatabaseValidationException identity、binding或schema错配时抛出；调用方必须保留marker并fail closed
     * @throws TableDdlStorageException 发现所有权漂移、重复键或物理树不一致时抛出；不得发布shadow
     */
    public TableStorageBinding reconcileOnlineShadowIdentity(
            StorageTableRebuildRequest request,
            TableStorageBinding shadowBinding,
            SearchKey clusteredIdentity) {
        // 1、exact schema/binding先完成交叉校验，再读取shadow；失败不触碰任一表空间。
        validateOnlineShadow(request, shadowBinding, clusteredIdentity);
        TableIndexMetadata sourceMetadata = indexMetadataFactory.createTable(
                request.sourceDefinition(), request.sourceBinding());
        TableIndexMetadata targetMetadata = indexMetadataFactory.createTable(
                request.targetDefinition(), shadowBinding);
        BTreeIndex targetClustered = refreshRoot(targetMetadata.clusteredIndex());
        List<SecondaryIndexMetadata> targetSecondaries = targetMetadata.secondaryIndexes().stream()
                .map(secondary -> new SecondaryIndexMetadata(
                        refreshRoot(secondary.index()), secondary.layout(), secondary.logicalUnique()))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        MiniTransaction shadowRead = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> oldShadow;
        try {
            oldShadow = btree.lookupIncludingDeleted(
                    shadowRead, targetClustered, clusteredIdentity);
            mtrManager.commit(shadowRead);
        } catch (RuntimeException failure) {
            rollbackIfBound(shadowRead, failure);
            throw failure;
        }

        if (oldShadow.isPresent()) {
            LogicalRecord oldRow = oldShadow.orElseThrow().record();
            // 1、secondary先行删除；重试时ABSENT是合法收敛态，不能因部分reconciliation重复消费而失败。
            for (int index = 0; index < targetSecondaries.size(); index++) {
                SecondaryIndexMetadata secondary = targetSecondaries.get(index);
                BTreeIndex after = removeShadowSecondaryIfPresent(secondary, oldRow);
                targetSecondaries.set(index, new SecondaryIndexMetadata(
                        after, secondary.layout(), secondary.logicalUnique()));
            }

            // 2、聚簇ownership与LOB free共享一个MTR；任一身份错配均拒绝继续插入current truth。
            Optional<LobFreeBatchPlan> lobFree = planShadowLobFree(
                    oldRow, targetClustered, shadowBinding);
            RedoBudgetWorkload deleteWorkload = BTreeRedoBudgetEstimator.structuralDelete(
                    targetClustered.rootLevel());
            if (lobFree.isPresent()) {
                deleteWorkload = deleteWorkload.plus(lobFree.orElseThrow().workload());
            }
            MiniTransaction delete = mtrManager.begin(mtrManager.budgetFor(
                    RedoBudgetPurpose.CLUSTERED_DELETE, deleteWorkload));
            try {
                BTreeDeleteResult deleted = btree.deleteClustered(
                        delete, targetClustered, clusteredIdentity,
                        oldRow.hiddenColumns().dbTrxId(), oldRow.hiddenColumns().dbRollPtr());
                if (!deleted.removed()) {
                    throw new TableDdlStorageException(
                            "online shadow clustered ownership changed during reconciliation");
                }
                targetClustered = deleted.indexAfter();
                if (lobFree.isPresent()) {
                    try (var ignored = delete.allowOutOfOrderPageLatch(
                            "online shadow reconciliation owns clustered leaf before target LOB/FSP cleanup")) {
                        lobStorage.freePlannedBatch(delete, lobFree.orElseThrow());
                    }
                }
                mtrManager.commit(delete);
            } catch (RuntimeException failure) {
                rollbackIfBound(delete, failure);
                throw failure;
            }
        }

        // 3、final X保证点查后source不会再变化；DELETE只留下shadow absent，INSERT/UPDATE重新走完整LOB投影。
        BTreeIndex sourceClustered = refreshRoot(sourceMetadata.clusteredIndex());
        MiniTransaction sourceRead = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> currentSource;
        try {
            currentSource = btree.lookup(sourceRead, sourceClustered, clusteredIdentity);
            mtrManager.commit(sourceRead);
        } catch (RuntimeException failure) {
            rollbackIfBound(sourceRead, failure);
            throw failure;
        }
        if (currentSource.isPresent()) {
            LogicalRecord sourceRow = currentSource.orElseThrow().record();
            RebuildProjection projection = projectRebuildRow(
                    sourceRow, request.rewrites(), sourceClustered,
                    request.sourceBinding(), targetClustered, shadowBinding);
            RebuildClusteredInsert inserted = insertRebuildClusteredRow(
                    refreshRoot(targetClustered), sourceRow, projection,
                    request.targetDefinition().schemaVersion());
            targetClustered = inserted.index();

            // 4、此时source truth稳定，logical UNIQUE必须在插入前裁决；不能沿用base-copy的宽松模式。
            for (int index = 0; index < targetSecondaries.size(); index++) {
                SecondaryIndexMetadata secondary = targetSecondaries.get(index);
                BTreeIndex after = insertShadowSecondary(
                        secondary, inserted.row(), true);
                targetSecondaries.set(index, new SecondaryIndexMetadata(
                        after, secondary.layout(), secondary.logicalUnique()));
            }
        }
        return shadowBinding(request.targetDefinition(), shadowBinding,
                targetClustered, targetSecondaries);
    }

    /**
     * 在FORWARD_ONLY之前双向验证source与shadow的聚簇逻辑值及全部target secondary集合完全一致。
     * 所有扫描都按exclusive continuation分批返回，不持page guard跨行比较或LOB读取。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>source→shadow分批点查每个clustered identity，hydrate两侧LOB并比较transform后的逻辑值。</li>
     *     <li>对每个匹配shadow行点查全部target secondary physical entry，拒绝missing、marked或wrong entry。</li>
     *     <li>shadow clustered→source反向点查，拒绝copy或rollback遗留的extra row。</li>
     *     <li>逐棵扫描target secondary including-deleted视图并回查shadow clustered，拒绝extra、marked及UNIQUE重复。</li>
     * </ol>
     *
     * @param request 本次operation冻结的source/target schema、binding与transform
     * @param shadowBinding reconciliation后的未发布shadow exact binding
     * @param batchSize 每次物化的最大行或entry数，必须为正
     * @throws DatabaseValidationException 参数或binding错配时抛出
     * @throws TableDdlStorageException 任一方向不等价时抛出，调用方不得越过forward fence
     */
    public void verifyOnlineShadow(StorageTableRebuildRequest request,
                                   TableStorageBinding shadowBinding,
                                   int batchSize) {
        // 1、验证入口不接受空identity，基础binding校验复用一个合法的空值无关SearchKey不可行，故显式校验aggregate。
        if (request == null || shadowBinding == null || batchSize <= 0
                || request.targetDefinition().tableId() != shadowBinding.tableId()
                || !request.targetDefinition().spaceId().equals(shadowBinding.spaceId())
                || !request.targetDefinition().path().toAbsolutePath().normalize()
                .equals(shadowBinding.path().toAbsolutePath().normalize())) {
            throw new DatabaseValidationException(
                    "online shadow verification request/binding/batch is invalid");
        }
        requireOpenedPath(request.sourceBinding(), "online shadow verification source");
        requireOpenedPath(shadowBinding, "online shadow verification target");
        TableIndexMetadata sourceMetadata = indexMetadataFactory.createTable(
                request.sourceDefinition(), request.sourceBinding());
        TableIndexMetadata targetMetadata = indexMetadataFactory.createTable(
                request.targetDefinition(), shadowBinding);
        BTreeIndex sourceClustered = refreshRoot(sourceMetadata.clusteredIndex());
        BTreeIndex targetClustered = refreshRoot(targetMetadata.clusteredIndex());
        List<SecondaryIndexMetadata> secondaries = targetMetadata.secondaryIndexes().stream()
                .map(secondary -> new SecondaryIndexMetadata(refreshRoot(secondary.index()),
                        secondary.layout(), secondary.logicalUnique())).toList();

        // 1、source→shadow：目标行必须存在，且比较hydrate后的逻辑值而不是tablespace-specific LOB reference。
        Optional<SearchKey> sourceContinuation = Optional.empty();
        while (true) {
            List<cn.zhangyis.db.storage.btree.BTreeLookupResult> batch = scanTreeBatch(
                    sourceClustered, sourceContinuation, batchSize, false);
            for (var sourceResult : batch) {
                LogicalRecord sourceRow = sourceResult.record();
                SearchKey identity = physicalKey(sourceRow, sourceClustered);
                LogicalRecord targetRow = lookupRequiredLive(
                        targetClustered, identity, "missing shadow clustered row");
                List<ColumnValue> expected = projectLogicalValues(
                        sourceRow, request.rewrites(), sourceClustered,
                        request.sourceBinding());
                List<ColumnValue> actual = hydrateLogicalValues(
                        targetRow, targetClustered, shadowBinding);
                if (!expected.equals(actual)) {
                    throw new TableDdlStorageException(
                            "online shadow verification found wrong clustered row");
                }
                // 2、每棵target secondary都必须恰有由当前shadow行派生的live exact entry。
                for (SecondaryIndexMetadata secondary : secondaries) {
                    LogicalRecord expectedEntry = secondary.layout().toEntry(targetRow, false);
                    LogicalRecord actualEntry = lookupRequiredIncludingDeleted(
                            secondary.index(), secondary.layout().physicalKey(expectedEntry),
                            "missing shadow secondary entry");
                    if (actualEntry.deleted() || !actualEntry.equals(expectedEntry)) {
                        throw new TableDdlStorageException(
                                "online shadow verification found marked/wrong secondary entry");
                    }
                }
            }
            if (batch.size() < batchSize) {
                break;
            }
            sourceContinuation = Optional.of(physicalKey(
                    batch.getLast().record(), sourceClustered));
        }

        // 3、shadow→source：任一target extra clustered row都会在此被发现。
        Optional<SearchKey> targetContinuation = Optional.empty();
        while (true) {
            List<cn.zhangyis.db.storage.btree.BTreeLookupResult> batch = scanTreeBatch(
                    targetClustered, targetContinuation, batchSize, true);
            for (var targetResult : batch) {
                if (targetResult.record().deleted()) {
                    throw new TableDdlStorageException(
                            "online shadow verification found delete-marked clustered row");
                }
                SearchKey identity = physicalKey(targetResult.record(), targetClustered);
                lookupRequiredLive(sourceClustered, identity,
                        "extra shadow clustered row");
            }
            if (batch.size() < batchSize) {
                break;
            }
            targetContinuation = Optional.of(physicalKey(
                    batch.getLast().record(), targetClustered));
        }

        // 4、secondary→clustered反向扫描封闭extra/marked entry；logical UNIQUE在稳定集合上最终裁决。
        for (SecondaryIndexMetadata secondary : secondaries) {
            verifyShadowSecondary(secondary, targetClustered, batchSize);
        }
    }

    /** 在任何页访问前交叉校验online shadow的逻辑与物理owner。 */
    private void validateOnlineShadow(StorageTableRebuildRequest request,
                                      TableStorageBinding shadowBinding,
                                      SearchKey clusteredIdentity) {
        if (request == null || shadowBinding == null || clusteredIdentity == null
                || request.targetDefinition().tableId() != shadowBinding.tableId()
                || !request.targetDefinition().spaceId().equals(shadowBinding.spaceId())
                || request.targetDefinition().schemaVersion()
                != shadowBinding.rowFormatVersion()
                || !request.targetDefinition().path().toAbsolutePath().normalize()
                .equals(shadowBinding.path().toAbsolutePath().normalize())) {
            throw new DatabaseValidationException(
                    "online shadow request/binding/identity is invalid");
        }
        requireOpenedPath(request.sourceBinding(), "online shadow reconciliation source");
        requireOpenedPath(shadowBinding, "online shadow reconciliation target");
    }

    /** 按旧shadow行投影physical key，并幂等删除一棵target secondary中的对应entry。 */
    private BTreeIndex removeShadowSecondaryIfPresent(
            SecondaryIndexMetadata metadata, LogicalRecord oldRow) {
        BTreeIndex current = refreshRoot(metadata.index());
        LogicalRecord entry = metadata.layout().toEntry(oldRow, false);
        SearchKey key = metadata.layout().physicalKey(entry);
        MiniTransaction read = mtrManager.beginReadOnly();
        Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> found;
        try {
            found = btree.lookupIncludingDeleted(read, current, key);
            mtrManager.commit(read);
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
        if (found.isEmpty()) {
            return current;
        }
        MiniTransaction remove = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.SECONDARY_INDEX,
                BTreeRedoBudgetEstimator.structuralDelete(current.rootLevel())));
        try {
            BTreeSecondaryRemovalResult result = found.orElseThrow().record().deleted()
                    ? btree.purgeDeleteMarkedSecondary(remove, current, key)
                    : btree.deletePublishedSecondary(remove, current, key);
            mtrManager.commit(remove);
            return result.indexAfter();
        } catch (RuntimeException failure) {
            rollbackIfBound(remove, failure);
            throw failure;
        }
    }

    /** 从shadow聚簇旧像冻结LOB ownership释放计划；空计划不申请FSP redo admission。 */
    private Optional<LobFreeBatchPlan> planShadowLobFree(
            LogicalRecord oldRow, BTreeIndex targetClustered,
            TableStorageBinding shadowBinding) {
        List<LobFreeTarget> targets = new ArrayList<>();
        for (int ordinal = 0; ordinal < oldRow.columnValues().size(); ordinal++) {
            ColumnValue value = oldRow.columnValues().get(ordinal);
            if (value instanceof ColumnValue.ExternalValue external) {
                targets.add(new LobFreeTarget(ordinal,
                        targetClustered.schema().column(ordinal).type(), external));
            }
        }
        if (targets.isEmpty()) {
            return Optional.empty();
        }
        SegmentRef segment = shadowBinding.lobSegment().orElseThrow(() ->
                new DatabaseValidationException(
                        "online shadow external row has no authoritative LOB segment"));
        return Optional.of(lobStorage.planFreeBatch(segment, targets));
    }

    /** 在稳定source truth下执行logical UNIQUE检查并插入一个target secondary entry。 */
    private BTreeIndex insertShadowSecondary(
            SecondaryIndexMetadata metadata, LogicalRecord targetRow,
            boolean enforceUnique) {
        BTreeIndex current = refreshRoot(metadata.index());
        LogicalRecord entry = metadata.layout().toEntry(targetRow, false);
        SearchKey logicalKey = metadata.layout().logicalKey(entry);
        boolean containsNull = logicalKey.values().stream()
                .anyMatch(ColumnValue.NullValue.class::isInstance);
        if (enforceUnique && metadata.logicalUnique() && !containsNull) {
            MiniTransaction uniqueRead = mtrManager.beginReadOnly();
            try {
                boolean duplicate = btree.scanSecondaryPrefixIncludingDeleted(
                                uniqueRead, new SecondaryIndexMetadata(
                                        current, metadata.layout(), true), logicalKey, 1)
                        .stream().anyMatch(candidate -> !candidate.record().deleted());
                mtrManager.commit(uniqueRead);
                if (duplicate) {
                    throw new SecondaryIndexBuildDuplicateKeyException(
                            "online shadow reconciliation found duplicate UNIQUE key: index="
                                    + current.indexId());
                }
            } catch (RuntimeException failure) {
                rollbackIfBound(uniqueRead, failure);
                throw failure;
            }
        }
        MiniTransaction insert = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.SECONDARY_INDEX,
                BTreeRedoBudgetEstimator.insert(current.rootLevel())));
        try {
            BTreeInsertResult result = btree.insertSecondary(insert, current, entry);
            mtrManager.commit(insert);
            return result.indexAfterInsert();
        } catch (RuntimeException failure) {
            rollbackIfBound(insert, failure);
            throw failure;
        }
    }

    /** 把结构写后的B+Tree快照按target definition逻辑顺序重新投影为DD可发布binding。 */
    private TableStorageBinding shadowBinding(
            StorageTableDefinition definition, TableStorageBinding owner,
            BTreeIndex clustered, List<SecondaryIndexMetadata> secondaries) {
        Map<Long, IndexStorageBinding> bindings = new LinkedHashMap<>();
        BTreeIndex currentClustered = refreshRoot(clustered);
        bindings.put(currentClustered.indexId(), indexBinding(currentClustered));
        for (SecondaryIndexMetadata secondary : secondaries) {
            BTreeIndex current = refreshRoot(secondary.index());
            bindings.put(current.indexId(), indexBinding(current));
        }
        List<IndexStorageBinding> ordered = definition.indexes().stream()
                .map(index -> Optional.ofNullable(bindings.get(index.indexId())).orElseThrow(() ->
                        new DatabaseValidationException(
                                "online shadow binding misses target index " + index.indexId())))
                .toList();
        return new TableStorageBinding(owner.tableId(), owner.spaceId(), owner.path(),
                owner.rowFormatVersion(), ordered, owner.lobSegment());
    }

    /** 在短只读MTR中扫描一批live或including-deleted记录，并在返回前释放全部页资源。 */
    private List<cn.zhangyis.db.storage.btree.BTreeLookupResult> scanTreeBatch(
            BTreeIndex index, Optional<SearchKey> continuation,
            int batchSize, boolean includingDeleted) {
        MiniTransaction scan = mtrManager.beginReadOnly();
        try {
            BTreeScanRange range = continuation
                    .map(key -> BTreeScanRange.after(key, batchSize))
                    .orElseGet(() -> BTreeScanRange.unbounded(batchSize));
            List<cn.zhangyis.db.storage.btree.BTreeLookupResult> result =
                    includingDeleted && !index.clustered()
                            ? btree.scanIncludingDeleted(scan, index, range)
                            : btree.scan(scan, index, range);
            mtrManager.commit(scan);
            return result;
        } catch (RuntimeException failure) {
            rollbackIfBound(scan, failure);
            throw failure;
        }
    }

    /** 点查一条live聚簇记录；缺失时用调用方提供的领域上下文拒绝发布。 */
    private LogicalRecord lookupRequiredLive(
            BTreeIndex index, SearchKey key, String failureMessage) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> result =
                    btree.lookup(read, index, key);
            mtrManager.commit(read);
            return result.orElseThrow(() ->
                    new TableDdlStorageException(failureMessage)).record();
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
    }

    /** 点查including-deleted索引视图，使验证能够区分缺失与残留marked entry。 */
    private LogicalRecord lookupRequiredIncludingDeleted(
            BTreeIndex index, SearchKey key, String failureMessage) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            Optional<cn.zhangyis.db.storage.btree.BTreeLookupResult> result =
                    btree.lookupIncludingDeleted(read, index, key);
            mtrManager.commit(read);
            return result.orElseThrow(() ->
                    new TableDdlStorageException(failureMessage)).record();
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
    }

    /** hydrate源行后按冻结rewrite生成不含物理LOB reference的target逻辑值。 */
    private List<ColumnValue> projectLogicalValues(
            LogicalRecord sourceRow, List<StorageColumnRewrite> rewrites,
            BTreeIndex sourceIndex, TableStorageBinding sourceBinding) {
        List<ColumnValue> sourceValues = hydrateLogicalValues(
                sourceRow, sourceIndex, sourceBinding);
        List<ColumnValue> projected = new ArrayList<>(rewrites.size());
        for (StorageColumnRewrite rewrite : rewrites) {
            projected.add(rewrite.sourceOrdinal() >= 0
                    ? sourceValues.get(rewrite.sourceOrdinal())
                    : toColumnValue(rewrite.defaultValue().orElseThrow(() ->
                    new DatabaseValidationException(
                            "online shadow verification cannot materialize REQUIRED added column"))));
        }
        return List.copyOf(projected);
    }

    /** 把一行中所有external值读取为逻辑值；每次LOB读取均在独立短只读MTR结束后返回。 */
    private List<ColumnValue> hydrateLogicalValues(
            LogicalRecord row, BTreeIndex index,
            TableStorageBinding binding) {
        List<ColumnValue> values = new ArrayList<>(row.columnValues().size());
        for (int ordinal = 0; ordinal < row.columnValues().size(); ordinal++) {
            ColumnValue value = row.columnValues().get(ordinal);
            values.add(value instanceof ColumnValue.ExternalValue external
                    ? hydrateRebuildLob(index.schema().column(ordinal).type(), binding, external)
                    : value);
        }
        return List.copyOf(values);
    }

    /** 反向验证一棵target secondary不含extra/marked/wrong entry，并最终裁决logical UNIQUE。 */
    private void verifyShadowSecondary(
            SecondaryIndexMetadata secondary, BTreeIndex targetClustered,
            int batchSize) {
        Optional<SearchKey> continuation = Optional.empty();
        while (true) {
            List<cn.zhangyis.db.storage.btree.BTreeLookupResult> batch = scanTreeBatch(
                    secondary.index(), continuation, batchSize, true);
            for (var result : batch) {
                LogicalRecord entry = result.record();
                if (entry.deleted()) {
                    throw new TableDdlStorageException(
                            "online shadow verification found marked secondary entry");
                }
                LogicalRecord row = lookupRequiredLive(targetClustered,
                        secondary.layout().clusterKey(entry),
                        "online shadow secondary points to absent clustered row");
                if (!secondary.layout().toEntry(row, false).equals(entry)) {
                    throw new TableDdlStorageException(
                            "online shadow verification found wrong secondary entry");
                }
                SearchKey logicalKey = secondary.layout().logicalKey(entry);
                if (secondary.logicalUnique() && logicalKey.values().stream()
                        .noneMatch(ColumnValue.NullValue.class::isInstance)) {
                    MiniTransaction uniqueRead = mtrManager.beginReadOnly();
                    try {
                        long duplicates = btree.scanSecondaryPrefixIncludingDeleted(
                                        uniqueRead, new SecondaryIndexMetadata(
                                                secondary.index(), secondary.layout(), true),
                                        logicalKey, 2).stream()
                                .filter(candidate -> !candidate.record().deleted()).count();
                        mtrManager.commit(uniqueRead);
                        if (duplicates != 1L) {
                            throw new SecondaryIndexBuildDuplicateKeyException(
                                    "online shadow verification found duplicate UNIQUE key: index="
                                            + secondary.index().indexId());
                        }
                    } catch (RuntimeException failure) {
                        rollbackIfBound(uniqueRead, failure);
                        throw failure;
                    }
                }
            }
            if (batch.size() < batchSize) {
                break;
            }
            continuation = Optional.of(secondary.layout().physicalKey(
                    batch.getLast().record()));
        }
    }

    /**
     * 将源行投影为目标 placeholder，并为每个需要 externalization 的目标列冻结新空间 LOB 计划。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 target ordinal 读取 source ordinal 或 ADD COLUMN typed default。</li>
     *     <li>遇到旧 external 时在独立只读 MTR 完整 hydrate，并核对引用属于源 binding 的 LOB segment。</li>
     *     <li>按目标列类型判断 inline/external；external 值只生成同长度 placeholder 和目标 segment 写计划。</li>
     *     <li>返回不持页资源的不可变投影；此阶段尚未在目标空间分配 LOB 页。</li>
     * </ol>
     *
     * @param source 当前批次已物化且不持 page guard 的 live 聚簇行
     * @param rewrites 与目标列 ordinal 一一对应的源位置/default 投影
     * @param sourceIndex committed 源聚簇 schema
     * @param sourceBinding 源 LOB ownership 的权威 binding
     * @param targetIndex shadow 目标聚簇 schema
     * @param targetBinding 尚未发布但基础 CREATE 已完成的目标 binding
     * @return placeholder values 与按 ordinal 排序的目标 LOB 写计划
     * @throws DatabaseValidationException default 缺失、LOB binding 错配或类型无法物化时抛出
     */
    private RebuildProjection projectRebuildRow(
            LogicalRecord source, List<StorageColumnRewrite> rewrites,
            BTreeIndex sourceIndex, TableStorageBinding sourceBinding,
            BTreeIndex targetIndex, TableStorageBinding targetBinding) {
        // 1、sourceOrdinal 始终指向 ALTER 开始时的 committed row format。
        List<ColumnValue> placeholders = new ArrayList<>(rewrites.size());
        List<PlannedRebuildLob> lobs = new ArrayList<>();
        for (int ordinal = 0; ordinal < rewrites.size(); ordinal++) {
            StorageColumnRewrite rewrite = rewrites.get(ordinal);
            ColumnValue value = rewrite.sourceOrdinal() >= 0
                    ? source.columnValues().get(rewrite.sourceOrdinal())
                    : toColumnValue(rewrite.defaultValue().orElseThrow(() ->
                    new DatabaseValidationException(
                            "non-empty rebuild row requires ADD COLUMN default")));

            // 2、旧 external 的物理 identity 绝不能进入 shadow record；先读取完整逻辑值并释放源 latch。
            if (value instanceof ColumnValue.ExternalValue external) {
                value = hydrateRebuildLob(
                        sourceIndex.schema().column(
                                rewrite.sourceOrdinal()).type(),
                        sourceBinding, external);
            }

            // 3、目标类型决定是否重新 externalize；计划绑定目标专属 LOB segment。
            ColumnType targetType =
                    targetIndex.schema().column(ordinal).type();
            if (targetType.storageKind() == StorageKind.OVERFLOW_CAPABLE
                    && !(value instanceof ColumnValue.NullValue)
                    && lobStorage.requiresExternalization(
                            targetType, value)) {
                Optional<SegmentRef> targetLob =
                        targetBinding.lobSegment();
                if (targetLob.isEmpty()) {
                    throw new DatabaseValidationException(
                            "shadow target requires a LOB segment at ordinal "
                                    + ordinal);
                }
                SegmentRef segment = targetLob.orElseThrow();
                LobWritePlan plan =
                        lobStorage.planWrite(segment, targetType, value);
                LobReference placeholderReference = new LobReference(
                        segment.spaceId(), PageNo.of(4L + ordinal),
                        plan.totalLength(), plan.pageCount(),
                        segment.segmentId(), segment.inodeSlot(),
                        plan.crc32());
                placeholders.add(new ColumnValue.ExternalValue(
                        targetType.typeId(), placeholderReference,
                        plan.inlinePrefix()));
                lobs.add(new PlannedRebuildLob(ordinal, plan));
            } else {
                placeholders.add(value);
            }
        }
        // 4、返回值只持逻辑值/冻结计划，不持源或目标页资源。
        return new RebuildProjection(
                List.copyOf(placeholders), List.copyOf(lobs));
    }

    /**
     * 读取一条旧 external LOB 并在返回前释放全部页 latch/fix。
     *
     * @param sourceType committed 源列类型
     * @param sourceBinding committed 源表 LOB ownership
     * @param external 源聚簇记录中的 external envelope
     * @return 可按目标 charset/type 重新编码的完整逻辑值
     */
    private ColumnValue hydrateRebuildLob(
            ColumnType sourceType, TableStorageBinding sourceBinding,
            ColumnValue.ExternalValue external) {
        SegmentRef segment = sourceBinding.lobSegment().orElseThrow(() ->
                new DatabaseValidationException(
                        "source external LOB has no authoritative segment"));
        LobReference reference = external.reference();
        if (!reference.spaceId().equals(segment.spaceId())
                || !reference.segmentId().equals(segment.segmentId())
                || reference.inodeSlot() != segment.inodeSlot()) {
            throw new DatabaseValidationException(
                    "source external LOB does not belong to source binding");
        }
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            ColumnValue value =
                    lobStorage.read(read, sourceType, external);
            mtrManager.commit(read);
            return value;
        } catch (RuntimeException failure) {
            rollbackIfBound(read, failure);
            throw failure;
        }
    }

    /**
     * 在一个目标写 MTR 中发布新 LOB ownership 与聚簇记录；源页资源已在调用前释放。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验源隐藏列，合并 B+Tree split 与全部 LOB 页链 redo 工作量后申请 admission。</li>
     *     <li>以 placeholder 固定聚簇路径/编码长度，再按 ordinal 写出目标 LOB 链。</li>
     *     <li>用真实 external envelope 发布聚簇行并转移 allocation ownership。</li>
     *     <li>提交 MTR 返回新 root/row/LSN；失败先补偿未转移 LOB，再关闭 prepare guard 并回滚 ACTIVE MTR。</li>
     * </ol>
     *
     * @param clustered 刷新过 root level 的目标聚簇 descriptor
     * @param sourceRow 提供 DB_TRX_ID/DB_ROLL_PTR 的源 live 行
     * @param projection 目标 placeholder 与冻结 LOB 计划
     * @param schemaVersion 目标 row format version
     * @return 已提交的目标聚簇 descriptor、真实行与 end LSN
     */
    private RebuildClusteredInsert insertRebuildClusteredRow(
            BTreeIndex clustered, LogicalRecord sourceRow,
            RebuildProjection projection, long schemaVersion) {
        // 1、保留原版本隐藏列，使 MVCC/undo identity 不因纯物理重建被伪造。
        var hidden = sourceRow.hiddenColumns();
        if (hidden == null || hidden.dbTrxId().isNone()) {
            throw new DatabaseValidationException(
                    "shadow rebuild source clustered row has no transaction identity");
        }
        RedoBudgetWorkload workload =
                BTreeRedoBudgetEstimator.insert(clustered.rootLevel());
        for (PlannedRebuildLob lob : projection.lobs()) {
            workload = workload.plus(lob.plan().workload());
        }
        MiniTransaction insert = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.CLUSTERED_INSERT, workload));
        PreparedClusteredInsert prepared = null;
        List<LobWriteAllocation> allocations =
                new ArrayList<>(projection.lobs().size());
        try {
            // 2、placeholder 与实际 external envelope 等长，prepare 可安全冻结 split/leaf 资源。
            LogicalRecord placeholder = new LogicalRecord(
                    schemaVersion, projection.placeholderValues(), false,
                    sourceRow.recordType(), hidden);
            prepared = btree.prepareClusteredInsert(
                    insert, clustered, placeholder, hidden.dbTrxId());
            List<ColumnValue> actualValues =
                    new ArrayList<>(projection.placeholderValues());
            for (PlannedRebuildLob lob : projection.lobs()) {
                LobWriteAllocation allocation =
                        lobStorage.writePlanned(insert, lob.plan());
                allocations.add(allocation);
                actualValues.set(lob.ordinal(), allocation.value());
            }

            // 3、row 成为新 ownership anchor 后，guard 转移；旧空间引用从未写入目标页。
            LogicalRecord actual = new LogicalRecord(
                    schemaVersion, actualValues, false,
                    sourceRow.recordType(), hidden);
            BTreeInsertResult result =
                    prepared.publish(actual, hidden.dbRollPtr());
            for (LobWriteAllocation allocation : allocations) {
                allocation.transferOwnership();
            }
            prepared.close();
            prepared = null;

            // 4、commit LSN 同时覆盖新 LOB/FSP 与聚簇 row；调用方随后才能发布二级 entry。
            Lsn lsn = mtrManager.commit(insert);
            return new RebuildClusteredInsert(
                    result.indexAfterInsert(), actual, lsn);
        } catch (RuntimeException failure) {
            for (int i = allocations.size() - 1; i >= 0; i--) {
                try {
                    allocations.get(i).close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (prepared != null) {
                try {
                    prepared.close();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            rollbackIfBound(insert, failure);
            throw failure;
        }
    }

    /** 从完整物理聚簇记录提取 continuation key；聚簇 key 唯一，exclusive lower 不会漏/重行。 */
    private static SearchKey physicalKey(
            LogicalRecord record, BTreeIndex index) {
        List<ColumnValue> values =
                new ArrayList<>(index.keyDef().parts().size());
        for (var part : index.keyDef().parts()) {
            values.add(record.columnValues().get(
                    part.columnId().value()));
        }
        return new SearchKey(values);
    }

    /**
     * 从 committed binding 与单个 staged descriptor 构造 online build 的 exact secondary metadata。
     *
     * @param definition 必须同时包含 committed indexes 与本次 staged secondary
     * @param existing 当前 committed binding
     * @param staged page3 owner/root/segment 快照
     * @return 与 staged index id 精确对应的 secondary descriptor/layout
     * @throws DatabaseValidationException identity、版本或索引集合不一致时抛出
     */
    private SecondaryIndexMetadata secondaryBuildMetadata(
            StorageTableDefinition definition, TableStorageBinding existing,
            SecondaryIndexBuildDescriptor staged) {
        if (definition == null || existing == null || staged == null
                || definition.tableId() != existing.tableId()
                || definition.tableId() != staged.tableId()
                || definition.schemaVersion() != existing.rowFormatVersion()
                || existing.indexes().stream().anyMatch(
                binding -> binding.indexId() == staged.indexBinding().indexId())) {
            throw new DatabaseValidationException("online secondary build metadata/identity is invalid");
        }
        requireOpenedPath(existing, "online CREATE INDEX mutation");
        List<IndexStorageBinding> bindings = new ArrayList<>(existing.indexes());
        bindings.add(staged.indexBinding());
        TableStorageBinding building = new TableStorageBinding(
                existing.tableId(), existing.spaceId(), existing.path(), existing.rowFormatVersion(),
                bindings, existing.lobSegment());
        return indexMetadataFactory.createTable(definition, building)
                .requireSecondary(staged.indexBinding().indexId());
    }

    /** 返回与 staged aggregate 同次映射的聚簇 descriptor，供 candidate current-read 与验证使用。 */
    private BTreeIndex clusteredBuildIndex(StorageTableDefinition definition,
                                           TableStorageBinding existing,
                                           SecondaryIndexBuildDescriptor staged) {
        // 复用 secondary helper 的完整交叉校验，再构造同一 binding aggregate，避免两个 helper 产生不同准入规则。
        secondaryBuildMetadata(definition, existing, staged);
        List<IndexStorageBinding> bindings = new ArrayList<>(existing.indexes());
        bindings.add(staged.indexBinding());
        TableStorageBinding building = new TableStorageBinding(
                existing.tableId(), existing.spaceId(), existing.path(), existing.rowFormatVersion(),
                bindings, existing.lobSegment());
        return indexMetadataFactory.createTable(definition, building).clusteredIndex();
    }

    /** 从结构写返回的 BTree descriptor 构造同 owner/version 的 page3 build descriptor。 */
    private static SecondaryIndexBuildDescriptor descriptorFor(
            SecondaryIndexBuildDescriptor owner, BTreeIndex index) {
        return new SecondaryIndexBuildDescriptor(owner.ddlOperationId(), owner.dictionaryVersion(),
                owner.tableId(), new IndexStorageBinding(index.indexId(), index.rootPageId(),
                index.rootLevel(), index.leafSegment(), index.nonLeafSegment()));
    }

    /** 一列目标 externalization 的 ordinal 与冻结写计划。 */
    private record PlannedRebuildLob(int ordinal, LobWritePlan plan) {
        private PlannedRebuildLob {
            if (ordinal < 0 || plan == null) {
                throw new DatabaseValidationException(
                        "invalid rebuild LOB plan");
            }
        }
    }

    /** 不持 page guard 的目标 placeholder 与有序 LOB 计划。 */
    private record RebuildProjection(
            List<ColumnValue> placeholderValues,
            List<PlannedRebuildLob> lobs) {
        private RebuildProjection {
            if (placeholderValues == null || lobs == null) {
                throw new DatabaseValidationException(
                        "invalid rebuild row projection");
            }
            placeholderValues = List.copyOf(placeholderValues);
            lobs = List.copyOf(lobs);
        }
    }

    /** 单行聚簇写的已提交结果，供随后二级 entry 构造。 */
    private record RebuildClusteredInsert(
            BTreeIndex index, LogicalRecord row, Lsn lsn) {
        private RebuildClusteredInsert {
            if (index == null || row == null || lsn == null) {
                throw new DatabaseValidationException(
                        "invalid rebuild clustered insert result");
            }
        }
    }

    /** storage API 常量 DTO 到 record 值的显式、穷尽映射。 */
    private static ColumnValue toColumnValue(StorageDefaultValue value) {
        return switch (value) {
            case StorageDefaultValue.NullValue ignored -> ColumnValue.NullValue.INSTANCE;
            case StorageDefaultValue.IntegerValue integer ->
                    new ColumnValue.IntValue(integer.value().longValue());
            case StorageDefaultValue.FloatingValue floating ->
                    new ColumnValue.DoubleValue(floating.value());
            case StorageDefaultValue.DecimalValue decimal ->
                    new ColumnValue.DecimalValue(decimal.value());
            case StorageDefaultValue.StringValue string ->
                    new ColumnValue.StringValue(string.value());
            case StorageDefaultValue.BytesValue bytes ->
                    new ColumnValue.BinaryValue(bytes.value());
            case StorageDefaultValue.TemporalValue temporal ->
                    new ColumnValue.TemporalValue(
                            cn.zhangyis.db.storage.record.type.TemporalKind.valueOf(
                                    temporal.kind().name()), temporal.value());
            case StorageDefaultValue.BitValue bits ->
                    new ColumnValue.BitValue(bits.value());
            case StorageDefaultValue.EnumValue enumeration ->
                    new ColumnValue.EnumValue(enumeration.ordinal());
            case StorageDefaultValue.SetValue set ->
                    new ColumnValue.SetValue(set.bitmap());
        };
    }

    /** 运行期 B+Tree descriptor 到可持久 DD binding 的纯映射。 */
    private static IndexStorageBinding indexBinding(BTreeIndex index) {
        return new IndexStorageBinding(index.indexId(), index.rootPageId(), index.rootLevel(),
                index.leafSegment(), index.nonLeafSegment());
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

    /** 把 storage.api DROP 所有权映射为 page3 通用 descriptor，不改变任何 identity。 */
    private static SdiIndexDdlDescriptor toSdi(SecondaryIndexDropDescriptor descriptor) {
        return new SdiIndexDdlDescriptor(SdiIndexDdlAction.DROP, descriptor.ddlOperationId(),
                descriptor.dictionaryVersion(), descriptor.tableId(), descriptor.indexBinding());
    }

    /** storage.api descriptor到页格式entry的无损映射。 */
    private static SdiOnlineAlterDescriptorEntry toSdiOnlineAlterEntry(
            OnlineAlterIndexDescriptor descriptor) {
        SdiOnlineAlterDescriptorAction action = switch (descriptor.action()) {
            case ADD -> SdiOnlineAlterDescriptorAction.ADD;
            case DROP -> SdiOnlineAlterDescriptorAction.DROP;
        };
        return new SdiOnlineAlterDescriptorEntry(
                action, descriptor.actionOrdinal(), descriptor.indexBinding());
    }

    /** 已完成页级CRC/space校验的entry到稳定storage.api视图的无损映射。 */
    private static OnlineAlterIndexDescriptor fromSdiOnlineAlterEntry(
            SdiOnlineAlterDescriptorEntry descriptor) {
        OnlineAlterIndexDescriptorAction action = switch (descriptor.action()) {
            case ADD -> OnlineAlterIndexDescriptorAction.ADD;
            case DROP -> OnlineAlterIndexDescriptorAction.DROP;
        };
        return new OnlineAlterIndexDescriptor(
                action, descriptor.actionOrdinal(), descriptor.indexBinding());
    }

    /** descriptor set到page3 anchor的精确owner映射。 */
    private static SdiOnlineAlterAnchor toSdiOnlineAlterAnchor(
            OnlineAlterDescriptorSet descriptors) {
        return new SdiOnlineAlterAnchor(
                descriptors.ddlOperationId(), descriptors.targetDictionaryVersion(),
                descriptors.tableId(), descriptors.generation(),
                descriptors.descriptorPages().getFirst().pageNo().value(),
                descriptors.descriptors().size(), descriptors.manifestDigest());
    }

    /** 按固定per-page容量从aggregate重建exact descriptor页，供CAS刷新与恢复比较共用。 */
    private static SdiOnlineAlterDescriptorPage descriptorPage(
            OnlineAlterDescriptorSet descriptors, int pageOrdinal, int perPage) {
        if (pageOrdinal < 0 || pageOrdinal >= descriptors.descriptorPages().size()
                || perPage <= 0) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor page ordinal/capacity is invalid");
        }
        int from = Math.multiplyExact(pageOrdinal, perPage);
        int to = Math.min(descriptors.descriptors().size(), from + perPage);
        long next = pageOrdinal + 1 < descriptors.descriptorPages().size()
                ? descriptors.descriptorPages().get(pageOrdinal + 1).pageNo().value() : 0L;
        return new SdiOnlineAlterDescriptorPage(
                descriptors.ddlOperationId(), descriptors.targetDictionaryVersion(),
                descriptors.tableId(), descriptors.generation(),
                descriptors.descriptorSegment(), pageOrdinal, next,
                descriptors.descriptors().subList(from, to).stream()
                        .map(TableDdlStorageService::toSdiOnlineAlterEntry).toList());
    }

    /** 比较全部恢复删除权限字段；manifest digest使用常量时间内容比较而非数组identity。 */
    private static boolean sameOnlineAlterDescriptorSet(
            OnlineAlterDescriptorSet left, OnlineAlterDescriptorSet right) {
        return left.ddlOperationId() == right.ddlOperationId()
                && left.targetDictionaryVersion() == right.targetDictionaryVersion()
                && left.tableId() == right.tableId()
                && left.generation() == right.generation()
                && left.descriptorSegment().equals(right.descriptorSegment())
                && left.descriptorPages().equals(right.descriptorPages())
                && left.descriptors().equals(right.descriptors())
                && MessageDigest.isEqual(left.manifestDigest(), right.manifestDigest());
    }

    /** rollback/finish共用的纯参数、table与space owner校验。 */
    private static void validateOnlineAlterDescriptorCleanup(
            TableStorageBinding table, OnlineAlterDescriptorSet expected,
            Duration timeout, String operation) {
        if (table == null || expected == null || timeout == null
                || timeout.isZero() || timeout.isNegative()
                || table.tableId() != expected.tableId()
                || !table.spaceId().equals(expected.descriptorSegment().spaceId())) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor " + operation
                            + " requires matching table/owner/positive timeout");
        }
    }

    /**
     * 在MTR/页副作用前验证通用descriptor请求的完整source ownership与ordinal唯一性。
     */
    private static void validateOnlineAlterDescriptorRequest(
            TableStorageBinding table, long ddlOperationId, long targetDictionaryVersion,
            long generation, List<OnlineAlterIndexAddRequest> additions,
            List<OnlineAlterIndexDropRequest> drops, byte[] manifestDigest,
            Duration timeout) {
        if (table == null || ddlOperationId <= 0 || targetDictionaryVersion <= 0
                || generation <= 0 || additions == null || drops == null
                || additions.isEmpty() && drops.isEmpty()
                || manifestDigest == null || manifestDigest.length != 32
                || timeout == null || timeout.isZero() || timeout.isNegative()
                || table.indexes().isEmpty()) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor request identity/actions/digest/timeout is invalid");
        }
        java.util.Set<Integer> ordinals = new java.util.HashSet<>();
        java.util.Set<Long> indexIds = new java.util.HashSet<>();
        for (IndexStorageBinding existing : table.indexes()) {
            indexIds.add(existing.indexId());
        }
        for (OnlineAlterIndexAddRequest addition : additions) {
            if (addition == null || !ordinals.add(addition.actionOrdinal())
                    || !indexIds.add(addition.definition().indexId())) {
                throw new DatabaseValidationException(
                        "online ALTER ADD ordinal/index identity is duplicate");
            }
        }
        IndexStorageBinding clustered = table.indexes().getFirst();
        for (OnlineAlterIndexDropRequest drop : drops) {
            if (drop == null || !ordinals.add(drop.actionOrdinal())
                    || drop.binding().equals(clustered)
                    || !table.indexes().contains(drop.binding())
                    || !drop.binding().rootPageId().spaceId().equals(table.spaceId())) {
                throw new DatabaseValidationException(
                        "online ALTER DROP must reference a current non-clustered binding");
            }
        }
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
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param leaf 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @param nonLeaf 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code indexDropWorkload} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static RedoBudgetWorkload indexDropWorkload(SegmentDropPlan leaf, SegmentDropPlan nonLeaf) {
        try {
            // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
            long fragments = Math.addExact(leaf.fragmentPageCount(), nonLeaf.fragmentPageCount());
            long extents = Math.addExact(leaf.extentCount(), nonLeaf.extentCount());
            // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
            long used = Math.addExact(leaf.usedPageCount(), nonLeaf.usedPageCount());
            long images = Math.addExact(12L, Math.multiplyExact(fragments, 4L));
            // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
            images = Math.addExact(images, Math.multiplyExact(extents, 6L));
            images = Math.addExact(images, Math.multiplyExact(used, 2L));
            // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
            return RedoBudgetWorkload.pageImages(images);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("secondary index drop redo workload overflows", overflow);
        }
    }

    /** 任意数量segment回收的保守redo预算；等待前已由短读MTR冻结每个plan。 */
    private static RedoBudgetWorkload onlineAlterDropWorkload(
            List<SegmentDropPlan> plans) {
        if (plans == null || plans.isEmpty() || plans.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "online ALTER segment drop plans must not be empty/null");
        }
        try {
            long images = 8L;
            for (SegmentDropPlan plan : plans) {
                images = Math.addExact(images,
                        Math.multiplyExact(plan.fragmentPageCount(), 4L));
                images = Math.addExact(images,
                        Math.multiplyExact(plan.extentCount(), 6L));
                images = Math.addExact(images,
                        Math.multiplyExact(plan.usedPageCount(), 2L));
                images = Math.addExact(images, 4L);
            }
            return RedoBudgetWorkload.pageImages(images);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor cleanup redo workload overflows", overflow);
        }
    }

    private static SecondaryIndexBuildDescriptor fromSdi(SdiIndexBuildDescriptor descriptor) {
        return new SecondaryIndexBuildDescriptor(descriptor.ddlOperationId(), descriptor.dictionaryVersion(),
                descriptor.tableId(), descriptor.indexBinding());
    }

    /** 把已校验 action 的 page3 descriptor 映射为稳定 storage.api DROP 所有权。 */
    private static SecondaryIndexDropDescriptor fromSdiDrop(SdiIndexDdlDescriptor descriptor) {
        return new SecondaryIndexDropDescriptor(descriptor.ddlOperationId(),
                descriptor.dictionaryVersion(), descriptor.tableId(), descriptor.indexBinding());
    }

    /** DROP rollback/finish 共用的纯参数与 table identity 校验。 */
    private static void validateSecondaryIndexDropArguments(
            TableStorageBinding table, SecondaryIndexDropDescriptor expected,
            Duration timeout, String action) {
        if (table == null || expected == null || timeout == null
                || timeout.isZero() || timeout.isNegative()
                || table.tableId() != expected.tableId()) {
            throw new DatabaseValidationException(
                    "secondary index drop " + action
                            + " requires matching table/descriptor/positive timeout");
        }
    }

    /**
     * 删除已由 DD 标记不可见的物理表。独占 operation lease 阻止新 MTR 进入；drain 后写 DISCARDED marker 并
     * flushThrough，随后失效所有 frame、关闭 FileChannel 再删除。任何失败都保留 fail-closed 状态供恢复续作。
     *
     * @param binding 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
    public void dropTable(TableStorageBinding binding, Duration timeout) {
        dropTable(binding, timeout, TableDdlStorageFaultInjector.NO_OP);
    }

    /**
     * 将 GENERAL 表空间 durable 标记为 DISCARDED、排空 BufferPool 并原子移动到 quarantine。
     * DD 状态和 DDL log 由上层 DictionaryDdlService 拥有，本方法只产生物理结果。
     *
     * @param binding 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param quarantine 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TableDdlStorageException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public void discardTablespace(TableStorageBinding binding, Path quarantine, Duration timeout) {
        if (binding == null || quarantine == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("discard binding/quarantine/positive timeout required");
        }
        requireOpenedPath(binding, "DISCARD TABLESPACE");
        if (changeBufferBarrier != null) {
            changeBufferBarrier.discardSpace(binding.spaceId(), timeout);
        }
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            TablespaceDrainResult drained = flush.drainTablespace(binding.spaceId(), timeout);
            if (drained.timedOut()) {
                throw new TableDdlStorageException("timed out draining tablespace before discard: " + binding.spaceId().value());
            }
            if (disk.tablespaceState(binding.spaceId()) != TablespaceState.DISCARDED) {
                MiniTransaction marker = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_TABLE_DROP));
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
            }
            pool.invalidateTablespace(binding.spaceId(), timeout);
            disk.closeTablespace(binding.spaceId());
            fileTransfer.discard(binding.path(), quarantine);
            log.info("discarded physical tablespace: table={} space={} quarantine={}", binding.tableId(),
                    binding.spaceId().value(), quarantine);
        }
    }

    /**
     * 不读取 page0 地把已隔离表空间移入受控 quarantine。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验写闸门、参数和绝对规范路径，拒绝符号链接，避免 FORCE 导出实例或路径替换绕过隔离。</li>
     *     <li>取得该 SpaceId 的独占 operation lease，再证明 registry 未打开且 Buffer Pool 没有任何 resident frame。</li>
     *     <li>按 source/destination 存在矩阵执行：两者同时存在即失败；source 缺失视为恢复重试完成；否则原子移动。</li>
     *     <li>只记录物理终点，不读取、修复或相信损坏 page0；DD 状态由上层 DDL log 状态机另行发布。</li>
     * </ol>
     *
     * @param binding DD 隔离表保存的稳定 table/space/canonical path 绑定
     * @param quarantine 由实例受控目录和 table/space identity 派生的唯一目标路径
     * @param timeout 调用方统一的正等待上界；当前文件动作不延长该上界
     * @throws DatabaseValidationException 参数或路径不满足离线约束时抛出
     * @throws TableDdlStorageException 发现打开句柄、缓存页、冲突文件或原子移动失败时抛出
     */
    public void discardRecoveryUnavailable(
            TableStorageBinding binding, Path quarantine, Duration timeout) {
        // 1. raw 路径没有 MTR 兜底，必须先检查统一写闸门和不可被目录穿越替换的路径身份。
        validateOfflineArguments(binding, quarantine, timeout, "recovery discard");
        writeAdmission.assertWriteAllowed();
        if (changeBufferBarrier != null) {
            changeBufferBarrier.discardUnavailableSpace(binding.spaceId(), timeout);
        }
        Path source = checkedOfflinePath(binding.path(), "recovery discard source");
        Path target = checkedOfflinePath(quarantine, "recovery discard target");
        if (source.equals(target)) {
            throw new DatabaseValidationException("recovery discard source and target must differ");
        }

        // 2. 独占 lease 阻止并发重新挂载；隔离空间正常启动不会进入 registry，也不得残留 frame。
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            assertOffline(binding.spaceId(), "recovery discard");

            // 3. 存在矩阵是恢复幂等性的唯一裁决，不解析可能损坏的 page0。
            boolean sourceExists = Files.exists(source, LinkOption.NOFOLLOW_LINKS);
            boolean targetExists = Files.exists(target, LinkOption.NOFOLLOW_LINKS);
            if (sourceExists && targetExists) {
                throw new TableDdlStorageException(
                        "recovery discard source and target both exist: " + source + " / " + target);
            }
            if (!sourceExists) {
                return;
            }
            try {
                Files.createDirectories(target.getParent());
                Files.move(source, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException failure) {
                throw new TableDdlStorageException(
                        "atomically move recovery-unavailable tablespace failed: " + source, failure);
            }

            // 4. 上层只有在本方法稳定返回后才能推进 ENGINE_DONE/字典终态。
            log.info("discarded recovery-unavailable tablespace: table={} space={} quarantine={}",
                    binding.tableId(), binding.spaceId().value(), target);
        }
    }

    /**
     * 不读取 page0 地幂等删除已隔离 canonical 文件；可信备份目录不属于本方法所有权。
     *
     * @param binding DD 隔离表保存的稳定物理绑定
     * @param timeout 调用方统一的正等待上界
     * @throws DatabaseValidationException 参数或路径非法时抛出
     * @throws TableDdlStorageException 目标仍在线、仍有缓存页或删除失败时抛出
     */
    public void dropRecoveryUnavailable(TableStorageBinding binding, Duration timeout) {
        dropRecoveryUnavailable(binding, binding == null ? null : binding.path(), timeout);
    }

    /**
     * 删除隔离对象当前拥有的 exact 文件；RECOVERY_DISCARDED 调用方传固定 quarantine，可信备份目录永不传入。
     *
     * @param binding DD 中保留的原始 canonical identity
     * @param source 本次状态实际拥有的 canonical 或固定 discarded 文件路径
     * @param timeout 调用方统一的正等待上界
     */
    public void dropRecoveryUnavailable(
            TableStorageBinding binding, Path source, Duration timeout) {
        validateOfflineArguments(binding, source, timeout, "recovery drop");
        writeAdmission.assertWriteAllowed();
        if (changeBufferBarrier != null) {
            changeBufferBarrier.discardUnavailableSpace(binding.spaceId(), timeout);
        }
        Path checkedSource = checkedOfflinePath(source, "recovery drop source");
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            assertOffline(binding.spaceId(), "recovery drop");
            try {
                Files.deleteIfExists(checkedSource);
            } catch (IOException failure) {
                throw new TableDdlStorageException(
                        "delete recovery-unavailable tablespace failed: " + checkedSource, failure);
            }
            log.info("dropped recovery-unavailable tablespace: table={} space={} path={}",
                    binding.tableId(), binding.spaceId().value(), checkedSource);
        }
    }

    /**
     * 为 ACTIVE 表生成稳定可信备份数据文件；manifest 与 HMAC 由 DD recovery-backup 层在本方法返回后最后发布。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验写准入、打开 binding 与全新受控目标，取得 SpaceId 独占 lease 阻断新的 MTR。</li>
     *     <li>drain 脏页并 force canonical 文件，以 checkpoint LSN 建立稳定 clean-copy 边界。</li>
     *     <li>复制到同目录临时文件，在副本 page0 写 DISCARDED lifecycle、重盖 checksum 并 force。</li>
     *     <li>原子发布数据文件，重新检查 page0 identity 与整文件 hash；失败删除临时文件且不产生 manifest。</li>
     * </ol>
     *
     * @param binding DD 锁内重读的 ACTIVE 物理绑定
     * @param target recovery-backups 根内尚不存在的最终数据文件
     * @param timeout drain 与 cache 协作使用的正上界
     * @return 已 force 且可由 manifest 签名的不可变文件证据
     * @throws TableDdlStorageException drain、复制、page0 改写、force 或 hash 失败时抛出
     */
    public RecoveryBackupFile createRecoveryBackupFile(
            TableStorageBinding binding, Path target, Duration timeout) {
        // 1. 备份会创建持久文件，导出只读/关闭闸门必须在取得资源前拒绝。
        validateOfflineArguments(binding, target, timeout, "recovery backup");
        writeAdmission.assertWriteAllowed();
        requireOpenedPath(binding, "recovery backup");
        Path normalizedTarget = checkedOfflinePath(target, "recovery backup target");
        Path temporary = normalizedTarget.resolveSibling(
                normalizedTarget.getFileName() + ".tmp");
        if (Files.exists(normalizedTarget, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(temporary)) {
            throw new TableDdlStorageException(
                    "recovery backup target already exists or temporary is unsafe: " + normalizedTarget);
        }

        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            // 2. X lease 内没有新页修改；drain+force 后 canonical bytes 对本次复制保持稳定。
            TablespaceDrainResult drained = flush.drainTablespace(binding.spaceId(), timeout);
            if (drained.timedOut()) {
                throw new TableDdlStorageException(
                        "timed out draining tablespace before recovery backup: "
                                + binding.spaceId().value());
            }
            store.force(binding.spaceId());

            // 3. 只修改副本生命周期；在线 canonical 始终保持 NORMAL，失败也不会影响服务文件。
            try {
                Files.createDirectories(normalizedTarget.getParent());
                Files.deleteIfExists(temporary);
                Files.copy(binding.path(), temporary, StandardCopyOption.COPY_ATTRIBUTES);
                markBackupCopyDiscarded(temporary);
                Files.move(temporary, normalizedTarget, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException failure) {
                throw new TableDdlStorageException(
                        "create recovery backup data file failed: " + normalizedTarget, failure);
            } finally {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException cleanupFailure) {
                    log.warn("failed to clean recovery backup temporary file: {}", temporary, cleanupFailure);
                }
            }

            // 4. 原子发布后从副本本身重建 identity/hash，manifest 不信任内存里的源文件假设。
            TablespaceFileIdentity identity = fileInspection.inspect(
                    normalizedTarget, binding.spaceId(), pageSize);
            try {
                return new RecoveryBackupFile(
                        normalizedTarget, identity, drained.checkpointLsn(),
                        Files.size(normalizedTarget), sha256(normalizedTarget));
            } catch (IOException failure) {
                throw new TableDdlStorageException(
                        "read recovery backup evidence failed: " + normalizedTarget, failure);
            }
        }
    }

    /**
     * 启动 DDL recovery 在 DD 已处于终态前重新挂载 ENGINE_DONE 的 NORMAL replacement；不产生写或 redo。
     *
     * @param binding marker 与 DD 共同确认的 replacement canonical binding
     * @throws TableDdlStorageException 文件无法按 NORMAL page0 身份挂载时抛出并阻止开放流量
     */
    public void mountRecoveryReplacement(TableStorageBinding binding) {
        if (binding == null) {
            throw new DatabaseValidationException("recovery replacement binding must not be null");
        }
        Path path = checkedOfflinePath(binding.path(), "recovery replacement path");
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            assertOffline(binding.spaceId(), "recovery replacement mount");
            disk.openTablespaceForRecovery(binding.spaceId(), path);
            if (disk.tablespaceState(binding.spaceId()) != TablespaceState.NORMAL) {
                disk.closeTablespace(binding.spaceId());
                throw new TableDdlStorageException(
                        "recovery replacement page0 is not NORMAL: " + binding.spaceId().value());
            }
        }
    }

    /**
     * 挂载尚未进入 committed DD 的 Online ALTER shadow，只供启动 DDL recovery 读取 target SDI。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验正 identity 与离线规范路径；不创建文件、不解析 SDI，也不申请 redo。</li>
     *     <li>取得该 space 的独占访问 lease 并证明尚未在线，避免与已发现表空间重名。</li>
     *     <li>按 recovery 模式打开文件并要求 page0 生命周期为 NORMAL；失败关闭刚打开句柄。</li>
     * </ol>
     *
     * @param tableId manifest 中冻结的正表标识，仅用于错误上下文与后续 SDI 交叉验证
     * @param spaceId manifest 中冻结且未被 committed DD 使用的 shadow space
     * @param path 受控 tables 目录内由 marker/manifest 双重验证的 exact shadow 路径
     * @throws DatabaseValidationException identity/path 缺失或路径含符号链接时抛出
     * @throws TableDdlStorageException space 已在线、page0 非 NORMAL 或文件无法打开时抛出并阻止 OPEN
     */
    public void mountOnlineAlterShadowForRecovery(
            long tableId, SpaceId spaceId, Path path) {
        // 1、该入口不接受完整binding，因为恢复读取SDI前尚不知道target indexes/segments。
        if (tableId <= 0 || spaceId == null || path == null) {
            throw new DatabaseValidationException(
                    "online ALTER recovery mount requires table/space/path");
        }
        Path checked = checkedOfflinePath(path, "online ALTER recovery shadow");
        // 2、exclusive lease与offline断言阻止覆盖已由discovery打开的committed空间。
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(spaceId)) {
            assertOffline(spaceId, "online ALTER recovery shadow mount");
            // 3、只有NORMAL shadow可作为READY/RECONCILED的发布候选。
            disk.openTablespaceForRecovery(spaceId, checked);
            if (disk.tablespaceState(spaceId) != TablespaceState.NORMAL) {
                disk.closeTablespace(spaceId);
                throw new TableDdlStorageException(
                        "online ALTER recovery shadow is not NORMAL: table=" + tableId
                                + " space=" + spaceId.value());
            }
        }
    }

    /**
     * 删除未被 committed DD 引用且未打开的 Online ALTER 残留表空间。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 marker 级 identity、正 timeout 与离线规范路径，拒绝符号链接目标。</li>
     *     <li>取得 space 独占 lease 并证明未在线；恢复器必须已用 DD/control/digest 决定回滚或退休。</li>
     *     <li>幂等删除 exact 文件；不存在视为已完成，IO 失败保留文件并阻止 marker 终结。</li>
     * </ol>
     *
     * @param tableId marker 中的正表标识，仅用于稳定诊断
     * @param spaceId 待清理文件的正表空间标识
     * @param path 已由 DD recovery 限制在 tables 根内的 exact 路径
     * @param timeout 调用方统一的正恢复预算；当前无等待 IO，但保留以稳定恢复 API 契约
     * @throws DatabaseValidationException 参数或路径无效时抛出且不删除文件
     * @throws TableDdlStorageException space 仍在线或 exact 文件删除失败时抛出
     */
    public void deleteUnopenedOnlineAlterTablespace(
            long tableId, SpaceId spaceId, Path path, Duration timeout) {
        // 1、删除资格来自上层持久状态机，本层只验证物理identity与边界。
        if (tableId <= 0 || spaceId == null || path == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "online ALTER recovery delete requires table/space/path/positive timeout");
        }
        writeAdmission.assertWriteAllowed();
        Path checked = checkedOfflinePath(path, "online ALTER recovery delete");
        // 2、绝不关闭一个已在线的同space句柄；调用方必须改走普通dropTable。
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(spaceId)) {
            assertOffline(spaceId, "online ALTER recovery delete");
            // 3、缺失是允许的崩溃重入状态；其余IO错误保持marker未终结。
            try {
                Files.deleteIfExists(checked);
            } catch (IOException failure) {
                throw new TableDdlStorageException(
                        "delete online ALTER recovery tablespace failed: " + checked, failure);
            }
            log.info("deleted unopened online ALTER tablespace: table={} space={} path={}",
                    tableId, spaceId.value(), checked);
        }
    }

    /** 在离线副本 page0 原地写 DISCARDED 并重盖页校验和，随后 force 数据与 metadata。 */
    private void markBackupCopyDiscarded(Path path) throws IOException {
        byte[] page = new byte[pageSize.bytes()];
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(page);
            long readPosition = 0;
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, readPosition);
                if (read < 0) {
                    throw new IOException("unexpected EOF reading recovery backup page0");
                }
                readPosition += read;
            }
            TablespaceLifecycleHeader current = TablespaceLifecycleRawCodec
                    .read(ByteBuffer.wrap(page)).orElseThrow(() ->
                            new TableDdlStorageException(
                                    "ACTIVE backup source lacks lifecycle marker: " + path));
            if (current.state() != TablespaceState.NORMAL) {
                throw new TableDdlStorageException(
                        "recovery backup source page0 is not NORMAL: " + current.state());
            }
            TablespaceLifecycleHeader discarded = new TablespaceLifecycleHeader(
                    TablespaceState.DISCARDED, current.initialSizeInPages(), 0,
                    current.initialSizeInPages(), TablespaceState.NORMAL);
            TablespaceLifecycleRawCodec.write(ByteBuffer.wrap(page), discarded);
            PageImageChecksum.stamp(page, pageSize);
            ByteBuffer output = ByteBuffer.wrap(page);
            long writePosition = 0;
            while (output.hasRemaining()) {
                writePosition += channel.write(output, writePosition);
            }
            channel.force(true);
        }
    }

    /** 流式计算整文件 SHA-256，避免把大型表空间一次性载入堆。 */
    private static String sha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException | IOException failure) {
            throw new TableDdlStorageException(
                    "compute recovery backup SHA-256 failed: " + path, failure);
        }
    }

    /**
     * 校验外部 DISCARDED 文件、复制到 canonical path，并恢复 page0 NORMAL。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 binding、候选文件 page0 身份与期望版本；失败时不消费旧 Change Buffer 证据也不复制文件。</li>
     *     <li>在取得用户表空间独占 lease 前，从 system.ibd 丢弃旧 SpaceId incarnation 的全部 pending mutation，
     *         避免持有用户 lease 后反向访问系统空间。</li>
     *     <li>取得独占 lifecycle lease，原子安装文件、以 recovery 模式挂载并失效同 SpaceId 的旧 frame。</li>
     *     <li>按重复页公式逐页重建不可信 Change Buffer bitmap；每页独立提交，page0 仍保持 DISCARDED。</li>
     *     <li>用 redo 恢复 page0 NORMAL，等待覆盖 bitmap reset 与 restore 的 WAL durable 边界后 force 数据文件。</li>
     *     <li>刷新 registry 元数据并发布成功日志；任一失败保持 DD/文件可由既有 IMPORT recovery 重试。</li>
     * </ol>
     *
     * @param binding 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param source 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param expected 表空间文件或 segment 的稳定身份与生命周期快照；不得为 {@code null}，必须与已打开文件和当前 generation 一致
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TableDdlStorageException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public void importTablespace(TableStorageBinding binding, Path source, TablespaceFileIdentity expected,
                                 Duration timeout) {
        // 1、先证明外部文件属于当前 binding；校验失败不得破坏旧 incarnation 的恢复证据。
        if (binding == null || source == null || expected == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("import binding/source/identity/positive timeout required");
        }
        if (!expected.spaceId().equals(binding.spaceId())) {
            throw new DatabaseValidationException("import identity does not match binding space");
        }
        TablespaceFileIdentity actual = fileInspection.inspect(source, binding.spaceId(), expected.pageSize());
        if (!actual.equals(expected)) {
            throw new TableDdlStorageException("import tablespace file identity mismatch: " + source);
        }
        Path temporary = binding.path().resolveSibling(binding.path().getFileName() + ".import.tmp");

        // 2、全局树属于 system.ibd，必须在用户空间独占 lease 外清除，保持跨空间 lease/latch 单向顺序。
        if (changeBufferBarrier != null) {
            changeBufferBarrier.discardUnavailableSpace(binding.spaceId(), timeout);
        }

        // 3、独占 lease 阻止旧 SpaceId 在安装与 reset 窗口重新进入普通 MTR。
        try (TablespaceAccessLease ignored = accessController.acquireExclusive(binding.spaceId())) {
            fileTransfer.importFile(source, binding.path(), temporary);
            disk.openTablespaceForRecovery(binding.spaceId(), binding.path());
            pool.invalidateTablespace(binding.spaceId(), timeout);

            // 4、导入文件的 bitmap 属于其它运行历史；在 page0 NORMAL 前全部归零，不能据此承诺空闲空间。
            if (changeBufferBarrier != null) {
                changeBufferBarrier.resetImportedSpaceBitmaps(
                        binding.spaceId(), store.currentSizeInPages(binding.spaceId()), timeout);
            }

            // 5、restore redo 在 reset redo 之后追加；flushThrough 该 LSN 同时建立两者 WAL durable 边界。
            MiniTransaction restore = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_TABLE_DROP));
            Lsn restoreLsn;
            try {
                disk.restoreTablespace(restore, binding.spaceId());
                restoreLsn = mtrManager.commit(restore);
            } catch (RuntimeException failure) {
                rollbackIfBound(restore, failure);
                throw failure;
            }
            flush.flushThrough(restoreLsn, timeout);
            store.force(binding.spaceId());

            // 6、只有 reset、NORMAL marker 与数据文件 force 全部成功后才刷新普通访问 registry。
            disk.refreshTablespaceMetadata(binding.spaceId());
            log.info("imported physical tablespace: table={} space={} source={}", binding.tableId(),
                    binding.spaceId().value(), source);
        }
    }

    /**
     * 在 IMPORT 产生复制、打开或 redo 副作用前读取外部文件 page0，并使用当前实例页大小校验稳定身份。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验源路径与期望 space id，拒绝缺失身份且不创建临时文件。</li>
     *     <li>使用组合根固定的 page size 打开源文件，防止调用方通过自报页大小改变 framing。</li>
     *     <li>由只读 inspector 校验 page0 checksum、space id、类型和版本。</li>
     *     <li>返回不可变 identity；文件损坏时保留底层 cause 且不挂载 tablespace。</li>
     * </ol>
     *
     * @param source 实例受控 incoming 目录中的候选文件；不得为 {@code null}
     * @param expectedSpaceId DD 当前 binding 的稳定 space id；不得为 {@code null}
     * @return 从 page0 读取并完成完整性校验的文件身份
     * @throws DatabaseValidationException 参数缺失或 page0 身份与期望 space 不一致时抛出
     */
    public TablespaceFileIdentity inspectTablespaceFile(
            Path source, cn.zhangyis.db.domain.SpaceId expectedSpaceId) {
        // 1、纯参数错误不得触发文件打开。
        if (source == null || expectedSpaceId == null) {
            throw new DatabaseValidationException(
                    "tablespace inspection source/expected space must not be null");
        }
        // 2、页大小只来自已打开引擎的固定配置。
        PageSize configuredPageSize = pageSize;
        // 3、inspector 负责 page0 framing、checksum 与 identity 交叉校验。
        TablespaceFileIdentity identity =
                fileInspection.inspect(source, expectedSpaceId, configuredPageSize);
        // 4、返回只读身份；本方法不复制、挂载或修改候选文件。
        return identity;
    }

    /**
     * 测试可见的故障注入版本。先复核 DD binding path 与已打开 space 的真实文件一致，
     * 再进入任何 marker/close/delete 副作；避免损坏 catalog 令引擎标记一个 space 却删另一条路径。
     *
     * @param binding 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param faultInjector 由当前模块组合根提供的领域协作者；不得为 {@code null}，其状态和生命周期必须覆盖本次调用且不能绕过模块边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TableDdlStorageException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    void dropTable(TableStorageBinding binding, Duration timeout, TableDdlStorageFaultInjector faultInjector) {
        if (binding == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("drop binding/positive timeout required");
        }
        if (faultInjector == null) {
            throw new DatabaseValidationException("drop fault injector must not be null");
        }
        requireOpenedPath(binding, "DDL DROP");
        if (changeBufferBarrier != null) {
            changeBufferBarrier.discardSpace(binding.spaceId(), timeout);
        }
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

    /**
     * 校验当前状态后推进存储引擎稳定 API状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param definition 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
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

    /** 任何按 binding 读写物理页/文件的入口都先拒绝 catalog path 与真实句柄错绑。
     *
     * @param binding 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param operation 传给 {@code requireOpenedPath} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws TableDdlStorageException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    private void requireOpenedPath(TableStorageBinding binding, String operation) {
        Path openedPath = store.pathOf(binding.spaceId()).toAbsolutePath().normalize();
        Path bindingPath = binding.path().toAbsolutePath().normalize();
        if (!openedPath.equals(bindingPath)) {
            throw new TableDdlStorageException(operation + " binding path does not match opened tablespace: space="
                    + binding.spaceId().value() + " binding=" + bindingPath + " opened=" + openedPath);
        }
    }

    /** 校验离线动作的领域参数，所有失败都早于 lease 与文件副作用。 */
    private static void validateOfflineArguments(
            TableStorageBinding binding, Path path, Duration timeout, String operation) {
        if (binding == null || path == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(operation + " requires binding/path/positive timeout");
        }
    }

    /** 规范化离线路径并逐级拒绝符号链接；DD 层仍负责把路径限制在具体业务根目录。 */
    private static Path checkedOfflinePath(Path path, String role) {
        if (!path.isAbsolute()) {
            throw new DatabaseValidationException(role + " must be absolute: " + path);
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (hasSymbolicLinkComponent(normalized)) {
            throw new DatabaseValidationException(role + " must not be a symbolic link: " + normalized);
        }
        return normalized;
    }

    /** 检查绝对路径的全部既有分量，阻止 canonical/transfer 父目录被链接替换后的 raw IO。 */
    private static boolean hasSymbolicLinkComponent(Path path) {
        Path current = path.getRoot();
        for (Path component : path) {
            current = current == null ? component : current.resolve(component);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(current)) {
                return true;
            }
        }
        return false;
    }

    /** 独占 lease 内证明目标没有运行期句柄或缓存帧，避免 raw 文件动作与页访问交叉。 */
    private void assertOffline(cn.zhangyis.db.domain.SpaceId spaceId, String operation) {
        if (disk.isTablespaceOpen(spaceId)) {
            throw new TableDdlStorageException(operation + " requires unopened tablespace: " + spaceId.value());
        }
        boolean resident = pool.residentPageIds().stream()
                .anyMatch(pageId -> pageId.spaceId().equals(spaceId));
        if (resident) {
            throw new TableDdlStorageException(operation + " found resident buffer frames: " + spaceId.value());
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

    /** commit 失败会由 manager 自行解绑；仅仍绑定时才做 uncommitted rollback。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
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
