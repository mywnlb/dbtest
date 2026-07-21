package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;

import java.nio.file.Path;
import java.util.Optional;

/**
 * 一次 durable DDL phase marker。除 phase 外的字段在同一 ddl id 生命周期内不可变。
 *
 * @param marker 跨 DDL log、字典版本和受影响对象的稳定关联标记。
 * @param secondaryObjectId CREATE_INDEX/DROP_INDEX 的 index id；表级 DDL 固定为 0。
 * @param operation 当前 marker 的 operation-specific 状态机类型，包括表、索引与表空间 transfer DDL。
 * @param phase 当前单批原子发布的阶段。
 * @param spaceId 目标 file-per-table 物理空间。
 * @param path 目标表空间的绝对规范路径；恢复仍须通过受控目录校验。
 * @param auxiliaryPath 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
 * @param fileIdentity 可选的 {@code fileIdentity}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 */
public record DdlLogRecord(DdlUndoMarker marker, long secondaryObjectId,
                           DdlLogOperation operation, DdlLogPhase phase, SpaceId spaceId, Path path,
                           Optional<Path> auxiliaryPath, Optional<TablespaceFileIdentity> fileIdentity) {

    /**
     * 表级 DDL 的兼容构造器；其 secondary identity 固定为 0。
     */
    public DdlLogRecord(DdlUndoMarker marker, DdlLogOperation operation, DdlLogPhase phase,
                        SpaceId spaceId, Path path) {
        this(marker, 0L, operation, phase, spaceId, path, Optional.empty(), Optional.empty());
    }

    /** CREATE_INDEX/DROP_INDEX 调用方使用的 secondary identity 构造器。 */
    public DdlLogRecord(DdlUndoMarker marker, long secondaryObjectId, DdlLogOperation operation,
                        DdlLogPhase phase, SpaceId spaceId, Path path) {
        this(marker, secondaryObjectId, operation, phase, spaceId, path, Optional.empty(), Optional.empty());
    }

    /** 表级 transfer 的兼容构造器；secondary identity 固定为 0。 */
    public DdlLogRecord(DdlUndoMarker marker, DdlLogOperation operation, DdlLogPhase phase,
                        SpaceId spaceId, Path path, Optional<Path> auxiliaryPath,
                        Optional<TablespaceFileIdentity> fileIdentity) {
        this(marker, 0L, operation, phase, spaceId, path, auxiliaryPath, fileIdentity);
    }

    /**
     * 冻结恢复所需 identity；相对路径在进入 catalog 前转换为绝对规范路径。
     *
     * @throws DatabaseValidationException 字段缺失时抛出，不产生持久副作用。
     */
    public DdlLogRecord {
        validateFields(marker, secondaryObjectId, operation, phase, spaceId, path, auxiliaryPath, fileIdentity);
        path = path.toAbsolutePath().normalize();
        auxiliaryPath = auxiliaryPath == null ? Optional.empty() : auxiliaryPath.map(value -> value.toAbsolutePath().normalize());
        fileIdentity = fileIdentity == null ? Optional.empty() : fileIdentity;
        if ((operation == DdlLogOperation.DISCARD_TABLESPACE
                || operation == DdlLogOperation.IMPORT_TABLESPACE
                || operation == DdlLogOperation.IMPORT_RECOVERY_REPLACEMENT)
                && fileIdentity.isPresent() && !fileIdentity.orElseThrow().spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("tablespace transfer log identity does not match space");
        }
    }

    private static void validateFields(DdlUndoMarker marker, long secondaryObjectId, DdlLogOperation operation,
                                       DdlLogPhase phase, SpaceId spaceId, Path path,
                                       Optional<Path> auxiliaryPath, Optional<TablespaceFileIdentity> fileIdentity) {
        if (marker == null || operation == null || phase == null || spaceId == null || path == null) {
            throw new DatabaseValidationException("DDL log record fields must not be null");
        }
        boolean secondaryIdentityOperation = operation == DdlLogOperation.CREATE_INDEX
                || operation == DdlLogOperation.DROP_INDEX
                || operation == DdlLogOperation.REBUILD_TABLE;
        if (secondaryIdentityOperation && secondaryObjectId <= 0
                || !secondaryIdentityOperation && secondaryObjectId != 0) {
            throw new DatabaseValidationException("DDL secondary object identity does not match operation");
        }
        if (operation == DdlLogOperation.REBUILD_TABLE
                && (auxiliaryPath == null || auxiliaryPath.isEmpty())) {
            throw new DatabaseValidationException(
                    "REBUILD TABLE log requires a shadow auxiliary path");
        }
        if ((operation == DdlLogOperation.DISCARD_RECOVERY_UNAVAILABLE
                || operation == DdlLogOperation.IMPORT_RECOVERY_REPLACEMENT)
                && (auxiliaryPath == null || auxiliaryPath.isEmpty())) {
            throw new DatabaseValidationException(
                    operation + " log requires a controlled auxiliary path");
        }
        if (operation == DdlLogOperation.IMPORT_RECOVERY_REPLACEMENT
                && (fileIdentity == null || fileIdentity.isEmpty())) {
            throw new DatabaseValidationException(
                    "recovery replacement log requires validated file identity");
        }
    }

    /**
     * 保持全部不可变 identity、只替换 durable phase。
     *
     * @param next 由 repository 状态机验证过的直接后继阶段。
     * @return 携带相同 marker/operation/space/path 的新不可变 record。
     */
    public DdlLogRecord withPhase(DdlLogPhase next) {
        return new DdlLogRecord(marker, secondaryObjectId, operation, next, spaceId, path, auxiliaryPath, fileIdentity);
    }
}
