package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;

/**
 * Bound expression 的不可变结果类型。scalar 保存 exact DD 类型及表达式级 nullable，
 * boolean 单独表达 SQL 三值结果，避免把 MySQL BOOLEAN 的存储映射误当作谓词类型。
 */
public sealed interface BoundExpressionType
        permits BoundExpressionType.Scalar, BoundExpressionType.BooleanResult {

    /**
     * 返回表达式是否可能产生 SQL NULL/UNKNOWN。
     *
     * @return scalar 可产生 NULL 或 boolean 可产生 UNKNOWN 时为 {@code true}
     */
    boolean nullable();

    /**
     * exact DD scalar 类型及表达式级可空性。
     *
     * @param definition Binder 固定的完整列类型、charset/collation 与长度定义
     * @param nullable 当前表达式是否可能产生 SQL NULL；literal NULL 可使 NOT NULL 目标类型变为可空
     */
    record Scalar(ColumnTypeDefinition definition, boolean nullable)
            implements BoundExpressionType {
        public Scalar {
            if (definition == null) {
                throw new DatabaseValidationException(
                        "bound scalar type definition must not be null");
            }
        }
    }

    /**
     * SQL boolean 结果类型。
     *
     * @param nullable 表达式是否可能产生 UNKNOWN
     */
    record BooleanResult(boolean nullable) implements BoundExpressionType {
    }
}
