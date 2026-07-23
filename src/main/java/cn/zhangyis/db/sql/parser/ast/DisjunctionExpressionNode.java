package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

import java.util.List;

/**
 * 保持用户书写顺序的 SQL OR 语法节点。
 *
 * @param operands 至少两个 boolean operand；Parser 不在此实施 range union 或分支去重
 */
public record DisjunctionExpressionNode(
        List<BooleanExpressionNode> operands)
        implements BooleanExpressionNode {

    /**
     * 冻结至少两个 OR 分支；构造过程不执行排序、去重或范围推导。
     *
     * @param operands 按用户书写顺序排列的 boolean 分支
     * @throws DatabaseValidationException 列表缺失、元素不足或包含 Java null 时抛出
     */
    public DisjunctionExpressionNode {
        if (operands == null || operands.size() < 2
                || operands.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "boolean disjunction requires at least two operands");
        }
        operands = List.copyOf(operands);
    }

    /**
     * OR 诊断位置继承最左 operand，保持与用户文本的确定对应。
     *
     * @return 最左 operand 的稳定源位置
     */
    @Override
    public SourcePosition position() {
        return operands.getFirst().position();
    }
}
