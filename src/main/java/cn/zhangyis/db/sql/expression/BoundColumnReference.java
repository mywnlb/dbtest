package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * 已绑定到 exact table version 的列引用。
 *
 * @param columnId DD 列稳定身份；必须为正，防止 ordinal 漂移后读取另一列
 * @param columnOrdinal exact table version 中的零基位置
 * @param columnType Binder 读取的完整 DD 类型
 * @param position 用户列标识符的源起始位置
 */
public record BoundColumnReference(
        long columnId, int columnOrdinal, ColumnTypeDefinition columnType,
        SourcePosition position) implements BoundExpression {

    public BoundColumnReference {
        if (columnId <= 0 || columnOrdinal < 0
                || columnType == null || position == null) {
            throw new DatabaseValidationException(
                    "invalid bound column reference");
        }
    }

    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.Scalar(
                columnType, columnType.nullable());
    }
}
