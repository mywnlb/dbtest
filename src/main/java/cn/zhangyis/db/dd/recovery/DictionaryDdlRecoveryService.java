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
    private final DictionaryTablespaceDiscovery discovery;
    private final Path tablesDirectory;

    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory) {
        if (control == null || repository == null || cache == null || physical == null || tablesDirectory == null) {
            throw new DatabaseValidationException("dictionary DDL recovery collaborators/path must not be null");
        }
        this.control = control;
        this.repository = repository;
        this.cache = cache;
        this.physical = physical;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
        this.discovery = new DictionaryTablespaceDiscovery(repository, tablesDirectory);
    }

    /** 按 committed snapshot 顺序收敛 pending，再清理已决和孤儿文件；任一失败阻止 DatabaseEngine OPEN。 */
    public void recover(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("dictionary recovery timeout must be positive");
        }
        List<TableDefinition> pending = repository.snapshot().tables().values().stream()
                .filter(table -> table.state() == TableState.DROP_PENDING).toList();
        for (TableDefinition table : pending) {
            completePendingDrop(table, timeout);
        }
        cleanupDroppedAndOrphans();
    }

    private void completePendingDrop(TableDefinition pending, Duration timeout) {
        var binding = pending.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException("DROP_PENDING table has no storage binding: "
                        + pending.id().value()));
        Path path = discovery.checkedPath(binding.path());
        if (Files.exists(path)) {
            try {
                physical.dropTable(binding, timeout);
            } catch (RuntimeException e) {
                throw new DictionaryRecoveryException("resume physical DROP failed: table="
                        + pending.id().value(), e);
            }
        }
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
