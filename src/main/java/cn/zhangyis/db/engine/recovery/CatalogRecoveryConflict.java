package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.nio.file.Path;
import java.util.Optional;

/**
 * inspection 中一个可诊断、可选择（仅允许时）的冲突。
 *
 * @param id complete scan 内的稳定冲突 identity
 * @param kind 冲突类别及其隔离权限
 * @param path 具体证据路径；全局 manifest 状态可为空
 * @param detail 不包含可变对象引用的诊断文本
 */
public record CatalogRecoveryConflict(
        CatalogRecoveryConflictId id,
        CatalogRecoveryConflictKind kind,
        Optional<Path> path,
        String detail) {

    public CatalogRecoveryConflict {
        if (id == null || kind == null || path == null || detail == null || detail.isBlank()) {
            throw new DatabaseValidationException("catalog recovery conflict is invalid");
        }
        path = path.map(value -> value.toAbsolutePath().normalize());
    }

    /**
     * @return 该具体冲突是否携带路径且类别允许显式 quarantine
     */
    public boolean quarantinable() {
        return kind.quarantinable() && path.isPresent();
    }
}
