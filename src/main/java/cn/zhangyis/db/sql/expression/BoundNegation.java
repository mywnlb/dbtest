package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * Typed SQL NOT 表达式。
 *
 * @param operand 已完成类型绑定的 boolean operand
 * @param position 原始 NOT token 的源位置；规则改写必须保留该诊断身份
 */
public record BoundNegation(
        BoundExpression operand,
        SourcePosition position) implements BoundExpression {

    /**
     * 创建 typed NOT；成功后 nullable 与 operand 保持一致。
     *
     * @param operand 已绑定的 boolean operand
     * @param position 原始 NOT token 的源位置
     * @throws DatabaseValidationException operand 非 boolean 或字段缺失时抛出
     */
    public BoundNegation {
        if (operand == null || position == null
                || !(operand.type()
                instanceof BoundExpressionType.BooleanResult)) {
            throw new DatabaseValidationException(
                    "bound negation requires a boolean operand and source position");
        }
    }

    /**
     * NOT 不消除 UNKNOWN，结果 nullable 与 operand 一致。
     *
     * @return 保留 operand nullable 的 boolean 类型
     */
    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.BooleanResult(
                ((BoundExpressionType.BooleanResult)
                        operand.type()).nullable());
    }
}
