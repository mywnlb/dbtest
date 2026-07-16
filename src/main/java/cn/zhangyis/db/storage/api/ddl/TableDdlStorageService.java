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
     * 创建可立即打开的物理表。一个 MTR 同时初始化 page0/FSP、每个索引的两个 segment 与稳定 root，commit 后
     * 通过 flushThrough 建立 redo→data/checkpoint 屏障，只有该屏障成功才向 DD 返回 binding。
     */
    public TableStorageBinding createTable(StorageTableDefinition definition) {
        if (definition == null) {
            throw new DatabaseValidationException("storage table definition must not be null");
        }
        // 所有纯 schema/key 校验必须早于建文件和 MTR 页修改；失败时磁盘上不得出现半初始化 tablespace。
        for (StorageIndexDefinition index : definition.indexes()) {
            schemaMapper.tableSchema(definition, index.clustered());
            schemaMapper.indexKey(definition, index);
        }
        long pageImages;
        try {
            pageImages = Math.addExact(12L, Math.multiplyExact(6L, definition.indexes().size()));
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("DDL create redo workload overflows", overflow);
        }
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.DDL_TABLE_CREATE,
                RedoBudgetWorkload.pageImages(pageImages)));
        List<IndexStorageBinding> indexes = new ArrayList<>(definition.indexes().size());
        Lsn commitLsn;
        try {
            disk.createTablespace(mtr, definition.spaceId(), definition.path(), definition.initialSizeInPages(),
                    TablespaceType.GENERAL);
            for (StorageIndexDefinition index : definition.indexes()) {
                SegmentRef leaf = disk.createSegment(mtr, definition.spaceId(), SegmentPurpose.INDEX_LEAF);
                SegmentRef nonLeaf = disk.createSegment(mtr, definition.spaceId(), SegmentPurpose.INDEX_NON_LEAF);
                PageId root = disk.allocatePage(mtr, leaf);
                indexPages.createIndexPage(mtr, root, index.indexId(), 0);
                indexes.add(new IndexStorageBinding(index.indexId(), root, 0, leaf, nonLeaf));
            }
            commitLsn = mtrManager.commit(mtr);
        } catch (RuntimeException failure) {
            rollbackAndRemoveFailedCreate(mtr, definition, failure);
            throw failure;
        }
        flush.flushThrough(commitLsn, Duration.ofSeconds(30));
        store.force(definition.spaceId());
        log.info("created physical table: table={} space={} path={} indexes={}", definition.tableId(),
                definition.spaceId().value(), definition.path(), indexes.size());
        return new TableStorageBinding(definition.tableId(), definition.spaceId(), definition.path(), indexes);
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
