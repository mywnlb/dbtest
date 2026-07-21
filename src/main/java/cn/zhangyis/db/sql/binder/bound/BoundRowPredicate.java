package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.SqlValue;

/**
 * 已按 exact DD column 类型转换的单个 residual predicate。
 *
 * @param columnOrdinal 目标列在 exact table version 中的物理 ordinal
 * @param operator SQL 三值比较操作符
 * @param value 已完成范围、时区、ENUM/SET 等转换的右侧值；SQL NULL 用专用值对象表达
 */
public record BoundRowPredicate(int columnOrdinal, BoundRowPredicateOperator operator,
                                SqlValue value) {
    public BoundRowPredicate {
        if (columnOrdinal < 0 || operator == null || value == null) {
            throw new DatabaseValidationException("invalid bound row predicate");
        }
    }
}
