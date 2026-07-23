package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlValue;

/**
 * 已按 comparison 目标列完成 coercion 的 literal。
 *
 * @param value typed SQL 值；SQL NULL 使用 {@link SqlValue.NullValue}
 * @param targetType coercion 使用的 exact DD 类型，包含 charset/collation
 * @param position literal token 的源起始位置
 */
public record BoundLiteral(
        SqlValue value, ColumnTypeDefinition targetType,
        SourcePosition position) implements BoundExpression {

    public BoundLiteral {
        if (value == null || targetType == null || position == null) {
            throw new DatabaseValidationException("invalid bound literal");
        }
    }

    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.Scalar(
                targetType, value instanceof SqlValue.NullValue);
    }
}
