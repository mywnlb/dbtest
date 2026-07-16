package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
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
    private final StorageTableSchemaMapper schemaMapper = new StorageTableSchemaMapper();

    public TableDdlStorageService(MiniTransactionManager mtrManager, DiskSpaceManager disk,
                                  IndexPageAccess indexPages, BufferPool pool, PageStore store,
                                  FlushService flush, TablespaceAccessController accessController) {
        if (mtrManager == null || disk == null || indexPages == null || pool == null || store == null
                || flush == null || accessController == null) {
            throw new DatabaseValidationException("table DDL storage collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.disk = disk;
        this.indexPages = indexPages;
        this.pool = pool;
        this.store = store;
        this.flush = flush;
        this.accessController = accessController;
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
     * <p>与 MySQL/InnoDB 的差异：当前 v1 没有独立 {@code dd_ddl_log} 和 SDI，物理完成与字典发布之间的崩溃窗口
     * 依靠已提交 catalog binding 与受控命名 orphan discovery 收敛，而不是重放完整 Atomic DDL participant 状态机。</p>
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
            pageImages = Math.addExact(Math.addExact(12L, indexImages), requiresLobSegment ? 3L : 0L);
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
        return new TableStorageBinding(definition.tableId(), definition.spaceId(), definition.path(), indexes,
                lobSegment);
    }

    /** TEXT/BLOB/JSON 家族共用一个表级 LOB segment；VARCHAR/VARBINARY 仍只使用页内 variable 编码。 */
    private static boolean isLobCapable(StorageColumnTypeId typeId) {
        return switch (typeId) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
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
        Path openedPath = store.pathOf(binding.spaceId()).toAbsolutePath().normalize();
        Path bindingPath = binding.path().toAbsolutePath().normalize();
        if (!openedPath.equals(bindingPath)) {
            throw new TableDdlStorageException("DDL binding path does not match opened tablespace: space="
                    + binding.spaceId().value() + " binding=" + bindingPath + " opened=" + openedPath);
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
