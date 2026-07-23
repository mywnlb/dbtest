package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlBoolean;

/**
 * SQL boolean 三值常量。通常由规则从 NULL comparison 或纯常量 conjunction 产生。
 *
 * @param value 显式三值；不得使用 Java {@code null}
 * @param position 被折叠表达式的源起始位置
 */
public record BoundTruthLiteral(
        SqlBoolean value, SourcePosition position) implements BoundExpression {

    public BoundTruthLiteral {
        if (value == null || position == null) {
            throw new DatabaseValidationException(
                    "bound truth literal fields must not be null");
        }
    }

    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.BooleanResult(
                value == SqlBoolean.UNKNOWN);
    }
}
