package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一次 complete scan 内稳定定位具体冲突的短摘要标识。
 *
 * @param value 由 kind/path/detail 确定性生成的十六进制摘要
 */
public record CatalogRecoveryConflictId(String value) {
    public CatalogRecoveryConflictId {
        if (value == null || value.isBlank()) {
            throw new DatabaseValidationException("catalog recovery conflict id must not be blank");
        }
    }
}
