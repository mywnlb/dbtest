package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 启动前从 committed DD 快照发现 StorageEngine 必须在 crash recovery 阶段打开的表空间。 */
public final class DictionaryTablespaceDiscovery {

    private final PersistentDictionaryRepository repository;
    private final Path tablesDirectory;

    public DictionaryTablespaceDiscovery(PersistentDictionaryRepository repository, Path tablesDirectory) {
        if (repository == null || tablesDirectory == null) {
            throw new DatabaseValidationException("dictionary discovery repository/path must not be null");
        }
        this.repository = repository;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
    }

    /**
     * ACTIVE 文件缺失会破坏已提交表，直接 fail-closed；DROP_PENDING 文件允许已在崩溃前删除，恢复稍后只补 DROPPED。
     * DROPPED 文件不交给普通 StorageEngine 打开，由 DDL recovery 作为残留直接清理。
     */
    public List<TableStorageBinding> discover() {
        Set<cn.zhangyis.db.domain.SpaceId> spaces = new HashSet<>();
        return repository.snapshot().tables().values().stream()
                .filter(table -> table.state() == TableState.ACTIVE || table.state() == TableState.DROP_PENDING)
                .map(table -> binding(table, spaces))
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::orElseThrow)
                .toList();
    }

    private java.util.Optional<TableStorageBinding> binding(TableDefinition table,
                                                            Set<cn.zhangyis.db.domain.SpaceId> spaces) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException("committed table has no physical binding: " + table.id().value()));
        Path path = checkedPath(binding.path());
        if (!Files.exists(path)) {
            if (table.state() == TableState.ACTIVE) {
                throw new DictionaryRecoveryException("ACTIVE table file is missing: table=" + table.id().value()
                        + " path=" + path);
            }
            return java.util.Optional.empty();
        }
        if (!spaces.add(binding.spaceId())) {
            throw new DictionaryRecoveryException("duplicate DD tablespace id: " + binding.spaceId().value());
        }
        return java.util.Optional.of(binding);
    }

    /** 所有字典物理路径必须被限制在实例 tables 目录内，防止损坏 catalog 令恢复删除任意文件。 */
    public Path checkedPath(Path path) {
        if (path == null) {
            throw new DictionaryRecoveryException("dictionary tablespace path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(tablesDirectory)) {
            throw new DictionaryRecoveryException("dictionary tablespace path escapes tables directory: "
                    + normalized);
        }
        return normalized;
    }
}
