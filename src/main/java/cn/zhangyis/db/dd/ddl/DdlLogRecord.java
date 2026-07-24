package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;

import java.nio.file.Path;
import java.util.Optional;

/**
 * 一次 durable DDL phase/control marker。identity、protocol和schema checkpoint不可变；phase/control、
 * cancellation与retirement fence只能通过repository声明的单调转换更新。
 *
 * @param marker 跨 DDL log、字典版本和受影响对象的稳定关联标记。
 * @param secondaryObjectId CREATE_INDEX/DROP_INDEX 的 index id；表级 DDL 固定为 0。
 * @param operation 当前 marker 的 operation-specific 状态机类型，包括表、索引与表空间 transfer DDL。
 * @param phase 当前单批原子发布的阶段。
 * @param spaceId 目标 file-per-table 物理空间。
 * @param path 目标表空间的绝对规范路径；恢复仍须通过受控目录校验。
 * @param auxiliaryPath 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
 * @param fileIdentity 可选的 {@code fileIdentity}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 * @param executionProtocol blocking/online恢复协议；legacy只允许由旧格式decoder产生
 * @param sourceSchemaDigest DDL开始时committed source aggregate摘要；CREATE TABLE允许为空
 * @param intermediateSchemaDigest DROP/DISCARD/IMPORT pending aggregate摘要；其它operation为空
 * @param targetSchemaDigest 最终committed aggregate摘要；新production marker按operation策略要求存在
 * @param controlState durable取消/前滚方向；blocking/legacy marker保持OPEN但不开放control API
 * @param cancellation 仅CANCEL_REQUESTED存在的固定宽度取消信息
 * @param retirementFence target发布后旧资源的可选持久安全边界；从空到有只能写一次
 * @param batchManifest v5 批量 DROP 的完整冻结对象集合；其它 operation 必须为空
 */
public record DdlLogRecord(DdlUndoMarker marker, long secondaryObjectId,
                           DdlLogOperation operation, DdlLogPhase phase, SpaceId spaceId, Path path,
                           Optional<Path> auxiliaryPath, Optional<TablespaceFileIdentity> fileIdentity,
                           DdlExecutionProtocol executionProtocol,
                           Optional<DdlSchemaDigest> sourceSchemaDigest,
                           Optional<DdlSchemaDigest> intermediateSchemaDigest,
                           Optional<DdlSchemaDigest> targetSchemaDigest,
                           DdlControlState controlState,
                           Optional<DdlCancellation> cancellation,
                           Optional<DdlRetirementFence> retirementFence,
                           Optional<DdlBatchManifest> batchManifest) {

    /**
     * 表级 DDL 的兼容构造器；其 secondary identity 固定为 0。
     */
    public DdlLogRecord(DdlUndoMarker marker, DdlLogOperation operation, DdlLogPhase phase,
                        SpaceId spaceId, Path path) {
        this(marker, 0L, operation, phase, spaceId, path, Optional.empty(), Optional.empty(),
                DdlExecutionProtocol.LEGACY_PHASE_ONLY,
                Optional.empty(), Optional.empty(), Optional.empty(), DdlControlState.OPEN,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** CREATE_INDEX/DROP_INDEX 调用方使用的 secondary identity 构造器。 */
    public DdlLogRecord(DdlUndoMarker marker, long secondaryObjectId, DdlLogOperation operation,
                        DdlLogPhase phase, SpaceId spaceId, Path path) {
        this(marker, secondaryObjectId, operation, phase, spaceId, path, Optional.empty(), Optional.empty(),
                DdlExecutionProtocol.LEGACY_PHASE_ONLY,
                Optional.empty(), Optional.empty(), Optional.empty(), DdlControlState.OPEN,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** 表级 transfer 的兼容构造器；secondary identity 固定为 0。 */
    public DdlLogRecord(DdlUndoMarker marker, DdlLogOperation operation, DdlLogPhase phase,
                        SpaceId spaceId, Path path, Optional<Path> auxiliaryPath,
                        Optional<TablespaceFileIdentity> fileIdentity) {
        this(marker, 0L, operation, phase, spaceId, path, auxiliaryPath, fileIdentity,
                DdlExecutionProtocol.LEGACY_PHASE_ONLY,
                Optional.empty(), Optional.empty(), Optional.empty(), DdlControlState.OPEN,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** 旧源码中index/rebuild transfer使用的兼容构造器；只可用于legacy fixture/decoder。 */
    public DdlLogRecord(DdlUndoMarker marker, long secondaryObjectId, DdlLogOperation operation,
                        DdlLogPhase phase, SpaceId spaceId, Path path,
                        Optional<Path> auxiliaryPath,
                        Optional<TablespaceFileIdentity> fileIdentity) {
        this(marker, secondaryObjectId, operation, phase, spaceId, path, auxiliaryPath, fileIdentity,
                DdlExecutionProtocol.LEGACY_PHASE_ONLY,
                Optional.empty(), Optional.empty(), Optional.empty(), DdlControlState.OPEN,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * v4 调用点兼容构造器。既有单对象 marker 不携带 v5 batch manifest。
     *
     * @param marker DDL、版本和主对象 identity
     * @param secondaryObjectId 索引或 shadow identity；表级操作为零
     * @param operation 单对象 DDL operation
     * @param phase 当前 durable phase
     * @param spaceId 主物理空间
     * @param path 主受控路径
     * @param auxiliaryPath 可选辅助路径
     * @param fileIdentity 可选文件 identity
     * @param executionProtocol v4 执行协议
     * @param sourceSchemaDigest source checkpoint
     * @param intermediateSchemaDigest pending checkpoint
     * @param targetSchemaDigest target checkpoint
     * @param controlState 单调控制状态
     * @param cancellation 可选取消原因
     * @param retirementFence 可选退休资源屏障
     */
    public DdlLogRecord(
            DdlUndoMarker marker, long secondaryObjectId,
            DdlLogOperation operation, DdlLogPhase phase,
            SpaceId spaceId, Path path,
            Optional<Path> auxiliaryPath,
            Optional<TablespaceFileIdentity> fileIdentity,
            DdlExecutionProtocol executionProtocol,
            Optional<DdlSchemaDigest> sourceSchemaDigest,
            Optional<DdlSchemaDigest> intermediateSchemaDigest,
            Optional<DdlSchemaDigest> targetSchemaDigest,
            DdlControlState controlState,
            Optional<DdlCancellation> cancellation,
            Optional<DdlRetirementFence> retirementFence) {
        this(marker, secondaryObjectId, operation, phase, spaceId, path,
                auxiliaryPath, fileIdentity, executionProtocol,
                sourceSchemaDigest, intermediateSchemaDigest,
                targetSchemaDigest, controlState, cancellation,
                retirementFence, Optional.empty());
    }

    /**
     * 冻结恢复所需 identity；相对路径在进入 catalog 前转换为绝对规范路径。
     *
     * @throws DatabaseValidationException 字段缺失时抛出，不产生持久副作用。
     */
    public DdlLogRecord {
        validateFields(marker, secondaryObjectId, operation, phase, spaceId, path, auxiliaryPath, fileIdentity);
        if (executionProtocol == null || controlState == null) {
            throw new DatabaseValidationException("DDL log protocol/control must not be null");
        }
        path = path.toAbsolutePath().normalize();
        auxiliaryPath = auxiliaryPath == null ? Optional.empty() : auxiliaryPath.map(value -> value.toAbsolutePath().normalize());
        fileIdentity = fileIdentity == null ? Optional.empty() : fileIdentity;
        sourceSchemaDigest = sourceSchemaDigest == null ? Optional.empty() : sourceSchemaDigest;
        intermediateSchemaDigest = intermediateSchemaDigest == null ? Optional.empty() : intermediateSchemaDigest;
        targetSchemaDigest = targetSchemaDigest == null ? Optional.empty() : targetSchemaDigest;
        cancellation = cancellation == null ? Optional.empty() : cancellation;
        retirementFence = retirementFence == null ? Optional.empty() : retirementFence;
        batchManifest = batchManifest == null ? Optional.empty() : batchManifest;
        if ((controlState == DdlControlState.CANCEL_REQUESTED) != cancellation.isPresent()) {
            throw new DatabaseValidationException(
                    "DDL CANCEL_REQUESTED control and cancellation payload must appear together");
        }
        if (executionProtocol == DdlExecutionProtocol.LEGACY_PHASE_ONLY
                && (sourceSchemaDigest.isPresent() || intermediateSchemaDigest.isPresent()
                || targetSchemaDigest.isPresent() || controlState != DdlControlState.OPEN
                || cancellation.isPresent() || retirementFence.isPresent())) {
            throw new DatabaseValidationException("legacy DDL marker cannot carry v4 semantic fields");
        }
        if ((executionProtocol == DdlExecutionProtocol.ONLINE_INDEX_V1
                && operation != DdlLogOperation.CREATE_INDEX)
                || (executionProtocol == DdlExecutionProtocol.ONLINE_DROP_INDEX_V1
                && operation != DdlLogOperation.DROP_INDEX)
                || (executionProtocol == DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1
                && operation != DdlLogOperation.ALTER_TABLE_INPLACE)
                || (executionProtocol == DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1
                && operation != DdlLogOperation.REBUILD_TABLE)) {
            throw new DatabaseValidationException(
                    "DDL execution protocol does not match operation: " + executionProtocol + "/" + operation);
        }
        if (retirementFence.isPresent()
                && executionProtocol != DdlExecutionProtocol.ONLINE_DROP_INDEX_V1
                && executionProtocol != DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1
                && executionProtocol != DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1) {
            throw new DatabaseValidationException(
                    "DDL retirement fence requires a retirement-capable online protocol: "
                            + executionProtocol);
        }
        boolean batchOperation = operation == DdlLogOperation.DROP_TABLE_BATCH
                || operation == DdlLogOperation.DROP_SCHEMA_CASCADE;
        if (batchOperation != batchManifest.isPresent()
                || batchOperation
                && executionProtocol != DdlExecutionProtocol.BATCH_DROP_V1
                || !batchOperation
                && executionProtocol == DdlExecutionProtocol.BATCH_DROP_V1) {
            throw new DatabaseValidationException(
                    "DDL batch operation/protocol/manifest must appear together");
        }
        if (batchOperation) {
            validateBatchIdentity(marker, operation, spaceId, path,
                    batchManifest.orElseThrow());
        }
        if ((operation == DdlLogOperation.DISCARD_TABLESPACE
                || operation == DdlLogOperation.IMPORT_TABLESPACE
                || operation == DdlLogOperation.IMPORT_RECOVERY_REPLACEMENT)
                && fileIdentity.isPresent() && !fileIdentity.orElseThrow().spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("tablespace transfer log identity does not match space");
        }
    }

    /**
     * 交叉校验 marker 主 identity 与 manifest 首项/schema identity，阻止恢复时从两份矛盾证据猜测目标。
     */
    private static void validateBatchIdentity(
            DdlUndoMarker marker,
            DdlLogOperation operation,
            SpaceId spaceId,
            Path path,
            DdlBatchManifest manifest) {
        if (operation == DdlLogOperation.DROP_TABLE_BATCH) {
            if (manifest.schema().isPresent() || manifest.tables().isEmpty()
                    || marker.affectedObjectId()
                    != manifest.tables().getFirst().tableId().value()) {
                throw new DatabaseValidationException(
                        "DROP TABLE batch marker/manifest identity is invalid");
            }
        } else if (manifest.schema().isEmpty()
                || marker.affectedObjectId()
                != manifest.schema().orElseThrow().schemaId().value()) {
            throw new DatabaseValidationException(
                    "DROP SCHEMA cascade marker/manifest identity is invalid");
        }
        if (!manifest.tables().isEmpty()) {
            DdlBatchTableEntry first = manifest.tables().getFirst();
            if (!spaceId.equals(first.spaceId())
                    || path.getFileName() == null
                    || !path.getFileName().toString()
                    .equals(first.relativePath())) {
                throw new DatabaseValidationException(
                        "DDL batch marker primary path/space differs from manifest first table");
            }
        } else if (spaceId.value() != 0) {
            throw new DatabaseValidationException(
                    "empty DROP SCHEMA cascade marker must use system space identity zero");
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
        return new DdlLogRecord(marker, secondaryObjectId, operation, next, spaceId, path,
                auxiliaryPath, fileIdentity, executionProtocol,
                sourceSchemaDigest, intermediateSchemaDigest, targetSchemaDigest,
                controlState, cancellation, retirementFence, batchManifest);
    }

    /**
     * 保持phase和全部identity，只替换由repository CAS验证过的control/cancellation。
     *
     * @param next 单调后继CANCEL_REQUESTED或FORWARD_ONLY
     * @param nextCancellation 仅取消方向存在的固定宽度信息
     * @return 携带同一immutable/checkpoint/fence的新record
     */
    public DdlLogRecord withControl(DdlControlState next,
                                    Optional<DdlCancellation> nextCancellation) {
        return new DdlLogRecord(marker, secondaryObjectId, operation, phase, spaceId, path,
                auxiliaryPath, fileIdentity, executionProtocol,
                sourceSchemaDigest, intermediateSchemaDigest, targetSchemaDigest,
                next, nextCancellation, retirementFence, batchManifest);
    }

    /**
     * 保持phase/control和全部identity，从absent安装一次retirement fence。
     *
     * @param fence 已由operation barrier冻结且与marker identity匹配的持久边界
     * @return 携带同一字段和新fence的不可变record
     */
    public DdlLogRecord withRetirementFence(DdlRetirementFence fence) {
        return new DdlLogRecord(marker, secondaryObjectId, operation, phase, spaceId, path,
                auxiliaryPath, fileIdentity, executionProtocol,
                sourceSchemaDigest, intermediateSchemaDigest, targetSchemaDigest,
                controlState, cancellation, Optional.ofNullable(fence), batchManifest);
    }
}
