package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundExpressionType;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 逻辑过滤使用的规范谓词容器。condition 是唯一权威状态；conjunct 和引用列均由私有工厂
 * 从该表达式派生，调用方不能提交两份可能不一致的数据。
 */
public final class PredicateSet {
    /** 最终 SQL 真值权威；物理 range 只能缩小候选，不能替代该条件。 */
    private final BoundExpression condition;
    /** 按用户顺序递归展开 AND 的只读视图。 */
    private final List<BoundExpression> conjuncts;
    /** 首次出现顺序稳定的 table column ordinal 集。 */
    private final Set<Integer> referencedColumnOrdinals;

    private PredicateSet(BoundExpression condition) {
        this.condition = condition;
        ArrayList<BoundExpression> flattened = new ArrayList<>();
        flatten(condition, flattened);
        this.conjuncts = List.copyOf(flattened);
        LinkedHashSet<Integer> referenced = new LinkedHashSet<>();
        collectColumns(condition, referenced);
        this.referencedColumnOrdinals =
                java.util.Collections.unmodifiableSet(referenced);
    }

    /**
     * 从单一 boolean condition 建立派生视图。
     *
     * @param condition Binder 或规则产生的完整 WHERE 条件
     * @return condition 为唯一权威状态的不可变谓词集合
     * @throws DatabaseValidationException condition 缺失或不是 boolean 时抛出
     */
    public static PredicateSet of(BoundExpression condition) {
        if (condition == null || !(condition.type()
                instanceof BoundExpressionType.BooleanResult)) {
            throw new DatabaseValidationException(
                    "predicate set requires a non-null boolean condition");
        }
        return new PredicateSet(condition);
    }

    /**
     * 返回完整 SQL condition。
     *
     * @return 不可变且不为 {@code null} 的表达式根
     */
    public BoundExpression condition() {
        return condition;
    }

    /**
     * 返回按原顺序展开的 conjunction。
     *
     * @return 不可变且非空的 conjunct 视图；不执行排序或去重
     */
    public List<BoundExpression> conjuncts() {
        return conjuncts;
    }

    /**
     * 返回表达式引用的列 ordinal。
     *
     * @return 按首次出现顺序冻结的只读集合；truth-only 条件返回空集合
     */
    public Set<Integer> referencedColumnOrdinals() {
        return referencedColumnOrdinals;
    }

    private static void flatten(
            BoundExpression expression, List<BoundExpression> output) {
        if (expression instanceof BoundConjunction conjunction) {
            conjunction.operands().forEach(operand -> flatten(operand, output));
            return;
        }
        output.add(expression);
    }

    private static void collectColumns(
            BoundExpression expression, Set<Integer> output) {
        switch (expression) {
            case BoundColumnReference column ->
                    output.add(column.columnOrdinal());
            case BoundLiteral ignored -> {
                // literal 不引用 table column。
            }
            case BoundComparison comparison -> {
                collectColumns(comparison.left(), output);
                collectColumns(comparison.right(), output);
            }
            case BoundConjunction conjunction -> conjunction.operands()
                    .forEach(operand -> collectColumns(operand, output));
            case BoundDisjunction disjunction -> disjunction.operands()
                    .forEach(operand -> collectColumns(operand, output));
            case BoundNegation negation ->
                    collectColumns(negation.operand(), output);
            case BoundNullTest nullTest ->
                    collectColumns(nullTest.operand(), output);
            case BoundTruthLiteral ignored -> {
                // truth literal 不引用 table column。
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PredicateSet predicates
                && condition.equals(predicates.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition);
    }

    @Override
    public String toString() {
        return "PredicateSet[" + condition + "]";
    }
}
