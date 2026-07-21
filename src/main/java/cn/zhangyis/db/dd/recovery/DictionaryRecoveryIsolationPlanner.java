package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryIdRequest;
import cn.zhangyis.db.dd.repo.DictionarySnapshot;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.recovery.RecoveryUnavailableTable;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import cn.zhangyis.db.storage.recovery.RecoverySpaceExclusionPolicy;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * StorageEngine 打开任何用户文件前执行的对象级 FORCE 规划器。它以 committed DD 为唯一归属真相，
 * 将本次管理员 SpaceId 在一个字典事务内持久化为 RECOVERY_UNAVAILABLE，并为后续普通启动重建排除集合。
 */
public final class DictionaryRecoveryIsolationPlanner {

    /** 当前实例唯一 control 高水位，用于一次性 durable 预留隔离版本。 */
    private final DictionaryControlStore control;
    /** committed DD snapshot、事务提交和 unresolved DDL 证据来源。 */
    private final PersistentDictionaryRepository repository;
    /** 系统字典空间；任何模式都不得被对象级隔离。 */
    private final SpaceId dictionarySpaceId;
    /** 系统 undo 空间；跳过它会破坏恢复事务链，必须 fail-closed。 */
    private final SpaceId undoSpaceId;
    /** 用户 file-per-table 的唯一受控根目录。 */
    private final Path tablesDirectory;

    /**
     * 绑定启动阶段所需权威协作者；构造不读取文件或修改 DD。
     *
     * @param control 当前实例 control store
     * @param repository 当前实例 committed DD repository
     * @param dictionarySpaceId 固定系统字典空间
     * @param undoSpaceId 固定系统 undo 空间
     * @param tablesDirectory file-per-table 受控根目录
     */
    public DictionaryRecoveryIsolationPlanner(DictionaryControlStore control,
                                              PersistentDictionaryRepository repository,
                                              SpaceId dictionarySpaceId,
                                              SpaceId undoSpaceId,
                                              Path tablesDirectory) {
        if (control == null || repository == null || dictionarySpaceId == null || undoSpaceId == null
                || tablesDirectory == null) {
            throw new DatabaseValidationException("recovery isolation planner collaborators must not be null");
        }
        this.control = control;
        this.repository = repository;
        this.dictionarySpaceId = dictionarySpaceId;
        this.undoSpaceId = undoSpaceId;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
    }

    /**
     * 证明管理员空间到 committed ACTIVE/RECOVERY_UNAVAILABLE 对象的一对一归属，并原子发布新隔离状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>冻结 committed snapshot，建立覆盖所有 binding 的 SpaceId 反向索引；系统空间、重复 binding 与目录逃逸在任何写入前失败。</li>
     *     <li>仅在 FORCE 模式解析管理员集合；每项必须唯一命中 ACTIVE 或已隔离对象，且不得与任何 unresolved DDL 的对象、空间或路径相交。</li>
     *     <li>对仍为 ACTIVE 的全部目标只预留一个 dictionary version，并在单个 DictionaryTransaction 中逐表替换为 RECOVERY_UNAVAILABLE。</li>
     *     <li>重新读取 committed snapshot，生成 DD 排除集合和完整 unavailable 诊断；失败不允许进入用户 tablespace discovery。</li>
     * </ol>
     *
     * @param mode 当前启动恢复模式；非 FORCE 模式只重建既有 DD 隔离集合
     * @param administrativeSpaces 管理员本次明确声明的空间；仅 FORCE 可非空
     * @return 已完成 DD commit 的不可变隔离计划
     * @throws RecoveryIsolationException 归属未知/共享、状态非稳定、路径越界或目标与未决 DDL 相交时抛出
     */
    public RecoveryIsolationPlan plan(RecoveryMode mode, Set<SpaceId> administrativeSpaces) {
        if (mode == null || administrativeSpaces == null
                || administrativeSpaces.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovery isolation mode/spaces must not be null");
        }
        if (mode != RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE && !administrativeSpaces.isEmpty()) {
            throw new RecoveryIsolationException("administrative isolation requires FORCE recovery mode");
        }

        // 1. 反向索引包含 tombstone，SpaceId 曾经归属多个对象也视为歧义，不能凭文件是否存在猜 owner。
        DictionarySnapshot snapshot = repository.snapshot();
        Map<SpaceId, List<TableDefinition>> bySpace = new HashMap<>();
        Map<Path, List<TableDefinition>> byPath = new HashMap<>();
        for (TableDefinition table : snapshot.tables().values()) {
            table.storageBinding().ifPresent(binding -> {
                Path path = checkedPath(binding.path());
                bySpace.computeIfAbsent(binding.spaceId(), ignored -> new ArrayList<>()).add(table);
                byPath.computeIfAbsent(path, ignored -> new ArrayList<>()).add(table);
            });
        }
        byPath.forEach((path, owners) -> {
            if (owners.size() != 1) {
                throw new RecoveryIsolationException(
                        "recovery path must have exactly one committed DD owner: path="
                                + path + " owners=" + owners.size());
            }
        });

        // 2. 所有目标先完成纯读证明；只有完整集合安全时才允许 reserve/commit。
        List<TableDefinition> targets = new ArrayList<>();
        for (SpaceId spaceId : administrativeSpaces) {
            if (spaceId.equals(dictionarySpaceId) || spaceId.equals(undoSpaceId)) {
                throw new RecoveryIsolationException("system/undo tablespace cannot be isolated: " + spaceId.value());
            }
            List<TableDefinition> owners = bySpace.getOrDefault(spaceId, List.of());
            if (owners.size() != 1) {
                throw new RecoveryIsolationException("recovery space must have exactly one committed DD owner: space="
                        + spaceId.value() + " owners=" + owners.size());
            }
            TableDefinition target = owners.getFirst();
            if (target.state() != TableState.ACTIVE && target.state() != TableState.RECOVERY_UNAVAILABLE) {
                throw new RecoveryIsolationException("recovery target is not in a stable isolatable state: table="
                        + target.id().value() + " state=" + target.state());
            }
            assertNoIntersectingDdl(target);
            targets.add(target);
        }

        // 3. 一个版本、一个 catalog batch 是集合隔离的裁决点；任何表校验失败都会使整个事务无提交。
        List<TableDefinition> newlyUnavailable = targets.stream()
                .filter(table -> table.state() == TableState.ACTIVE).toList();
        if (!newlyUnavailable.isEmpty()) {
            long value = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 0, 1)).dictionaryVersion();
            DictionaryVersion version = DictionaryVersion.of(value);
            try (DictionaryTransaction transaction = repository.begin(version)) {
                for (TableDefinition table : newlyUnavailable) {
                    transaction.updateTable(new TableDefinition(table.id(), table.schemaId(), table.name(), version,
                            TableState.RECOVERY_UNAVAILABLE, table.columns(), table.indexes(),
                            table.storageBinding(), table.options()));
                }
                transaction.commit();
            }
        }

        // 4. 只从 commit 后 snapshot 生成长期排除证据；RECOVERY_DISCARDED 无 canonical 文件，不进入物理 skip 集合。
        DictionarySnapshot committed = repository.snapshot();
        Set<SpaceId> dictionarySpaces = new HashSet<>();
        List<RecoveryUnavailableTable> unavailable = new ArrayList<>();
        for (TableDefinition table : committed.tables().values()) {
            if (table.state() == TableState.RECOVERY_UNAVAILABLE
                    || table.state() == TableState.RECOVERY_DISCARDED) {
                TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                        new RecoveryIsolationException("recovery-isolated table has no binding: " + table.id().value()));
                if (table.state() == TableState.RECOVERY_UNAVAILABLE) {
                    dictionarySpaces.add(binding.spaceId());
                }
                SchemaDefinition schema = committed.schemas().get(table.schemaId());
                if (schema == null) {
                    throw new RecoveryIsolationException("recovery-isolated table schema is missing: "
                            + table.id().value());
                }
                unavailable.add(new RecoveryUnavailableTable(
                        new QualifiedTableName(ObjectName.of("def"), schema.name(), table.name()),
                        table.id(), table.state(), table.version(), binding));
            }
        }
        unavailable.sort(java.util.Comparator.comparingLong(value -> value.tableId().value()));
        return new RecoveryIsolationPlan(
                RecoverySpaceExclusionPolicy.of(administrativeSpaces, dictionarySpaces), unavailable);
    }

    /** 目标与任一未决 DDL identity/path 相交都拒绝，避免 FORCE 与 DDL recovery 对同一对象作相反裁决。 */
    private void assertNoIntersectingDdl(TableDefinition table) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new RecoveryIsolationException("recovery target has no physical binding: " + table.id().value()));
        Path targetPath = checkedPath(binding.path());
        for (DdlLogRecord record : repository.ddlLog().unresolved()) {
            boolean intersects = record.marker().affectedObjectId() == table.id().value()
                    || record.spaceId().equals(binding.spaceId())
                    || record.path().equals(targetPath)
                    || record.auxiliaryPath().map(path -> path.equals(targetPath)).orElse(false);
            if (intersects) {
                throw new RecoveryIsolationException("recovery target intersects unresolved DDL: table="
                        + table.id().value() + " ddl=" + record.marker().ddlOperationId());
            }
        }
    }

    /** 规范化并限制路径；任一已有路径分量为符号链接时拒绝，避免 raw DDL 经父目录链接逃逸受控目录。 */
    private Path checkedPath(Path path) {
        if (path == null) {
            throw new RecoveryIsolationException("recovery table path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(tablesDirectory)) {
            throw new RecoveryIsolationException("recovery table path escapes tables directory: " + normalized);
        }
        if (hasSymbolicLinkComponent(normalized)) {
            throw new RecoveryIsolationException("recovery table path must not be a symbolic link: " + normalized);
        }
        return normalized;
    }

    /** 沿绝对路径逐级使用 NOFOLLOW 语义检查，目标尚未创建时仍会检查其全部既有父目录。 */
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
}
