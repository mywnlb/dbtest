package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;

import java.nio.file.Path;

/**
 * 一次 durable DDL phase marker。除 phase 外的字段在同一 ddl id 生命周期内不可变。
 *
 * @param marker 跨 DDL log、字典版本和受影响对象的稳定关联标记。
 * @param secondaryObjectId CREATE_INDEX 预留的 index id；表级 DDL 固定为 0。
 * @param operation CREATE_TABLE、DROP_TABLE 或 CREATE_INDEX。
 * @param phase 当前单批原子发布的阶段。
 * @param spaceId 目标 file-per-table 物理空间。
 * @param path 目标表空间的绝对规范路径；恢复仍须通过受控目录校验。
 */
public record DdlLogRecord(DdlUndoMarker marker, long secondaryObjectId,
                           DdlLogOperation operation, DdlLogPhase phase, SpaceId spaceId, Path path) {

    /**
     * 表级 DDL 的兼容构造器；其 secondary identity 固定为 0。
     */
    public DdlLogRecord(DdlUndoMarker marker, DdlLogOperation operation, DdlLogPhase phase,
                        SpaceId spaceId, Path path) {
        this(marker, 0L, operation, phase, spaceId, path);
    }

    /**
     * 冻结恢复所需 identity；相对路径在进入 catalog 前转换为绝对规范路径。
     *
     * @throws DatabaseValidationException 字段缺失时抛出，不产生持久副作用。
     */
    public DdlLogRecord {
        if (marker == null || operation == null || phase == null || spaceId == null || path == null) {
            throw new DatabaseValidationException("DDL log record fields must not be null");
        }
        if (operation == DdlLogOperation.CREATE_INDEX && secondaryObjectId <= 0
                || operation != DdlLogOperation.CREATE_INDEX && secondaryObjectId != 0) {
            throw new DatabaseValidationException("DDL secondary object identity does not match operation");
        }
        path = path.toAbsolutePath().normalize();
    }

    /**
     * 保持全部不可变 identity、只替换 durable phase。
     *
     * @param next 由 repository 状态机验证过的直接后继阶段。
     * @return 携带相同 marker/operation/space/path 的新不可变 record。
     */
    public DdlLogRecord withPhase(DdlLogPhase next) {
        return new DdlLogRecord(marker, secondaryObjectId, operation, next, spaceId, path);
    }
}
