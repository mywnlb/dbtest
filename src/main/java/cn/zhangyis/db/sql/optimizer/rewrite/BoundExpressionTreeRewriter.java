package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * 对 sealed {@link BoundExpression} 执行统一 bottom-up 不可变重建。
 *
 * <p>规则只处理当前节点，不再各自复制递归逻辑。新增表达式种类会使本类的 sealed switch
 * 编译失败，从而强制实现者同时决定其子树遍历和重建语义。</p>
 */
final class BoundExpressionTreeRewriter {
    private BoundExpressionTreeRewriter() {
    }

    /**
     * 先改写全部 child，再把完整重建节点交给当前规则。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 sealed 节点种类递归改写 child，叶节点保持原引用。</li>
     *     <li>仅在 child 结构变化时重建父节点，保留类型与源位置不变量。</li>
     *     <li>把 bottom-up 结果交给单节点规则；规则不得返回 Java null。</li>
     * </ol>
     *
     * @param expression 当前完整 boolean/scalar 表达式
     * @param nodeRule 只改写当前节点的无状态规则
     * @return 保持不变时尽量复用引用的改写结果
     * @throws SqlOptimizationException 规则返回 Java null 时抛出，调用方不得发布部分计划
     */
    static BoundExpression rewrite(
            BoundExpression expression,
            UnaryOperator<BoundExpression> nodeRule) {
        // 1、先递归 child，保证 nodeRule 观察到的每个直接子树已完成本轮改写。
        BoundExpression withRewrittenChildren = switch (expression) {
            case BoundColumnReference column -> column;
            case BoundLiteral literal -> literal;
            case BoundTruthLiteral truth -> truth;
            case BoundComparison comparison -> {
                BoundExpression left =
                        rewrite(comparison.left(), nodeRule);
                BoundExpression right =
                        rewrite(comparison.right(), nodeRule);
                yield left.equals(comparison.left())
                        && right.equals(comparison.right())
                        ? comparison
                        : new BoundComparison(
                        left, comparison.operator(), right);
            }
            case BoundConjunction conjunction -> {
                List<BoundExpression> operands =
                        rewriteOperands(
                                conjunction.operands(), nodeRule);
                yield operands.equals(conjunction.operands())
                        ? conjunction
                        : new BoundConjunction(operands);
            }
            case BoundDisjunction disjunction -> {
                List<BoundExpression> operands =
                        rewriteOperands(
                                disjunction.operands(), nodeRule);
                yield operands.equals(disjunction.operands())
                        ? disjunction
                        : new BoundDisjunction(operands);
            }
            case BoundNegation negation -> {
                BoundExpression operand =
                        rewrite(negation.operand(), nodeRule);
                yield operand.equals(negation.operand())
                        ? negation
                        : new BoundNegation(
                        operand, negation.position());
            }
            case BoundNullTest nullTest -> {
                BoundExpression operand =
                        rewrite(nullTest.operand(), nodeRule);
                yield operand.equals(nullTest.operand())
                        ? nullTest
                        : new BoundNullTest(
                        operand, nullTest.operator(),
                        nullTest.position());
            }
        };
        // 2、构造器已重新校验父节点 type/arity；相等 child 不产生额外对象。
        // 3、规则协议损坏必须早于物理计划与 storage 资源发布。
        BoundExpression rewritten =
                nodeRule.apply(withRewrittenChildren);
        if (rewritten == null) {
            throw new SqlOptimizationException(
                    "expression rewrite rule returned null");
        }
        return rewritten;
    }

    /**
     * 按稳定顺序改写 n-ary boolean operand。
     *
     * @param operands 原始不可变 operand 列表
     * @param nodeRule 当前规则
     * @return 保持顺序的新不可变列表
     */
    private static List<BoundExpression> rewriteOperands(
            List<BoundExpression> operands,
            UnaryOperator<BoundExpression> nodeRule) {
        return operands.stream()
                .map(operand -> rewrite(operand, nodeRule))
                .toList();
    }
}
