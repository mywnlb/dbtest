package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryIdAllocation;
import cn.zhangyis.db.dd.repo.DictionaryIdRequest;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * StorageEngine crash recovery 成功、开放上层流量前执行的 DDL 收敛阶段。它续作 DROP_PENDING、删除 DROPPED
 * 残留与未被任何 committed DD binding 引用的受控命名 orphan；不执行 SQL，也不猜测 ACTIVE 缺失文件。
 */
@Slf4j
public final class DictionaryDdlRecoveryService {

    private final DictionaryControlStore control;
    private final PersistentDictionaryRepository repository;
    private final DictionaryObjectCache cache;
    private final TableDdlStorageService physical;
    /** DROP_PENDING 物理删除前再次校验的恢复期 history barrier。 */
    private final TablePurgeBarrier purgeBarrier;
    private final DictionaryTablespaceDiscovery discovery;
    private final Path tablesDirectory;

    /**
     * 构造不接 persistent history barrier 的低层恢复服务；只适用于没有真实 committed history 的组件测试。
     *
     * @param control         字典 id/version 的 durable 单调分配器。
     * @param repository      committed catalog 仓储。
     * @param cache           table metadata cache/invalidation 入口。
     * @param physical        物理 tablespace DROP facade。
     * @param tablesDirectory 受控表空间文件目录。
     * @throws DatabaseValidationException 任一依赖或目录为空时抛出。
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory) {
        this(control, repository, cache, physical, tablesDirectory, TablePurgeBarrier.NONE);
    }

    /**
     * 构造生产 DDL 恢复服务；StorageEngine 完成 history rebuild/RESUME_PURGE 后，每个 pending 物理删除前仍复核 barrier。
     *
     * @param control         字典 id/version 的 durable 单调分配器。
     * @param repository      committed catalog 仓储与 recovery snapshot 来源。
     * @param cache           table metadata cache/invalidation 入口。
     * @param physical        物理 tablespace DROP facade。
     * @param tablesDirectory 受控 tablespace 文件目录，构造时转为绝对规范路径。
     * @param purgeBarrier    与 StorageEngine history 投影共享 owner 的表级等待 API。
     * @throws DatabaseValidationException 任一依赖、目录或 barrier 为空时抛出。
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory,
                                        TablePurgeBarrier purgeBarrier) {
        if (control == null || repository == null || cache == null || physical == null || tablesDirectory == null
                || purgeBarrier == null) {
            throw new DatabaseValidationException("dictionary DDL recovery collaborators/path must not be null");
        }
        this.control = control;
        this.repository = repository;
        this.cache = cache;
        this.physical = physical;
        this.purgeBarrier = purgeBarrier;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
        this.discovery = new DictionaryTablespaceDiscovery(repository, tablesDirectory);
    }

    /**
     * 按 committed snapshot 顺序收敛 pending，再清理已决和孤儿文件；任一失败阻止 DatabaseEngine OPEN。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 timeout 并冻结当前 committed DROP_PENDING 列表。</li>
     *     <li>逐表等待 purge barrier、完成物理删除并发布 DROPPED。</li>
     *     <li>全部 pending 收敛后清理 DROPPED residue 与不被 committed binding 引用的受控 orphan。</li>
     * </ol>
     *
     * @param timeout 每张 DROP_PENDING 表等待 purge barrier 与物理 DROP 的正有界时间。
     * @throws DatabaseValidationException timeout 为空、零或负数时抛出，且不修改文件/catalog。
     * @throws DictionaryRecoveryException pending metadata/binding、barrier、物理 DROP、catalog publish 或 orphan 扫描失败时抛出；
     *                                     DatabaseEngine 必须保持上层流量关闭。
     */
    public void recover(Duration timeout) {
        // 1、恢复只消费 committed catalog snapshot；timeout 非正时不进入任何文件修改。
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("dictionary recovery timeout must be positive");
        }
        List<TableDefinition> pending = repository.snapshot().tables().values().stream()
                .filter(table -> table.state() == TableState.DROP_PENDING).toList();
        // 2、每张 pending 表独立复核 barrier；失败保留当前/后续文件并阻止上层 OPEN。
        for (TableDefinition table : pending) {
            completePendingDrop(table, timeout);
        }
        // 3、只有 pending 全部收敛后才做 orphan cleanup，避免半恢复时扩大文件变更范围。
        cleanupDroppedAndOrphans();
    }

    /**
     * 续作一张 DROP_PENDING 表。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验 pending storage binding/path，不猜测缺失 metadata。</li>
     *     <li>在物理删除前等待恢复出的 persistent history 引用归零；超时保持 DROP_PENDING 和文件。</li>
     *     <li>文件仍存在时调用 storage DDL facade 完成 WAL/doublewrite/invalidate/delete 生命周期。</li>
     *     <li>物理状态收敛后预留新版本并发布 DROPPED，同时失效 cache。</li>
     * </ol>
     *
     * @param pending committed catalog 中状态为 DROP_PENDING、仍携带 durable storage binding 的表版本。
     * @param timeout 当前恢复阶段为 barrier 与物理 DROP 提供的正有界等待时间。
     * @throws DictionaryRecoveryException binding/path 缺失、barrier 等待失败、物理删除或 DROPPED publish 失败时抛出。
     */
    private void completePendingDrop(TableDefinition pending, Duration timeout) {
        // 1、DROP_PENDING binding 在物理删除前仍是 purge/rollback resolver 的权威 metadata。
        var binding = pending.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException("DROP_PENDING table has no storage binding: "
                        + pending.id().value()));
        Path path = discovery.checkedPath(binding.path());

        // 2、即使 storage RESUME_PURGE 已运行，也必须按当前恢复投影再次复核，禁止越过残留引用。
        purgeBarrier.awaitUnreferenced(pending.id().value(), timeout);

        // 3、崩溃前文件可能已经删除；存在时才续作幂等物理 DROP。
        if (Files.exists(path)) {
            try {
                physical.dropTable(binding, timeout);
            } catch (RuntimeException e) {
                throw new DictionaryRecoveryException("resume physical DROP failed: table="
                        + pending.id().value(), e);
            }
        }
        // 4、文件已删除后以单调新版本发布 DROPPED，随后 cache 永久失效。
        DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 1, 1));
        DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
        TableDefinition dropped = new TableDefinition(pending.id(), pending.schemaId(), pending.name(), version,
                TableState.DROPPED, pending.columns(), pending.indexes(), pending.storageBinding());
        try (DictionaryTransaction transaction = repository.begin(version)) {
            transaction.updateTable(dropped);
            transaction.commit();
        }
        cache.invalidateTable(pending.id(), version);
        log.info("recovered pending DROP: table={} ddlId={} version={}", pending.id().value(),
                ids.firstDdlId(), version.value());
    }

    /**
     * 清理 committed DROPPED 表空间残留与未被 ACTIVE/DROP_PENDING binding 引用的受控命名 CREATE orphan。
     *
     * @throws DictionaryRecoveryException 路径越界、目录扫描或文件删除失败时抛出，恢复保持 fail-closed。
     */
    private void cleanupDroppedAndOrphans() {
        Set<Path> live = new HashSet<>();
        for (TableDefinition table : repository.snapshot().tables().values()) {
            table.storageBinding().ifPresent(binding -> {
                Path path = discovery.checkedPath(binding.path());
                if (table.state() == TableState.ACTIVE || table.state() == TableState.DROP_PENDING) {
                    live.add(path);
                } else {
                    delete(path, "DROPPED tablespace residue");
                }
            });
        }
        if (!Files.exists(tablesDirectory)) {
            return;
        }
        try (DirectoryStream<Path> files = Files.newDirectoryStream(tablesDirectory, "table_*_space_*.ibd")) {
            for (Path file : files) {
                Path normalized = discovery.checkedPath(file);
                if (!live.contains(normalized)) {
                    delete(normalized, "uncommitted CREATE orphan");
                }
            }
        } catch (IOException e) {
            throw new DictionaryRecoveryException("scan dictionary tables directory failed: " + tablesDirectory, e);
        }
    }

    /**
     * 幂等删除一个已经过受控路径校验的表空间文件，并保留删除原因用于恢复日志。
     *
     * @param path   待删除的规范化受控文件路径。
     * @param reason DROPPED residue 或 uncommitted CREATE orphan 的诊断分类。
     * @throws DictionaryRecoveryException 文件删除失败时抛出并保留 {@link IOException} 根因。
     */
    private static void delete(Path path, String reason) {
        try {
            if (Files.deleteIfExists(path)) {
                log.warn("deleted {}: {}", reason, path);
            }
        } catch (IOException e) {
            throw new DictionaryRecoveryException("delete " + reason + " failed: " + path, e);
        }
    }
}
