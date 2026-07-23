package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlBoolean;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 从规范 PredicateSet 派生安全列约束、exact equality 和 never-true 证明。
 *
 * <p>本类不改写表达式、不选择索引，也不复制 storage collation。无法证明的值序只保留
 * residual，不得收紧候选范围。</p>
 */
public final class PredicateAnalyzer {

    /**
     * 分析规范 conjunction，并保留完整 residual 作为最终 SQL 真值权威。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按原顺序读取 PredicateSet 的派生 conjunct，truth literal 只影响 empty 证明。</li>
     *     <li>要求 comparison 已规范为 column-literal，并按 ordinal 合并安全列约束。</li>
     *     <li>记录全部 equality 映射；重复 equality 继续作为 semantic IR 损坏失败。</li>
     *     <li>冻结分析结果；字符串等未知 collation 值序不会被当作矛盾或更严格边界。</li>
     * </ol>
     *
     * @param predicates 规则程序输出、condition 单一权威的谓词集合
     * @return 不可变约束、equality 与 empty 证明
     * @throws SqlOptimizationException comparison 未规范或重复 equality 时抛出；
     *         OR/NOT/null-test 作为 residual barrier，不属于错误
     */
    public PredicateAnalysis analyze(PredicateSet predicates) {
        // 1、truth-only condition 不下沉 endpoint；FALSE/UNKNOWN 在 WHERE 中均 never true。
        if (predicates == null) {
            throw new SqlOptimizationException(
                    "predicate set for analysis must not be null");
        }
        LinkedHashMap<Integer, ColumnConstraint> constraints =
                new LinkedHashMap<>();
        LinkedHashMap<Integer, SqlValue> equalities =
                new LinkedHashMap<>();
        boolean allEqualities = true;
        boolean empty = false;
        for (BoundExpression expression : predicates.conjuncts()) {
            if (expression instanceof BoundTruthLiteral truth) {
                if (truth.value() != SqlBoolean.TRUE) {
                    empty = true;
                }
                allEqualities = false;
                continue;
            }
            // 2、OR/NOT/null-test 不提供 M3 range 证明，但完整 residual 仍由物理计划保留。
            if (!(expression instanceof BoundComparison comparison)) {
                allEqualities = false;
                continue;
            }
            // comparison 属于已支持叶类型，方向损坏不能伪装成 opaque residual。
            if (!(comparison.left()
                    instanceof BoundColumnReference column)
                    || !(comparison.right()
                    instanceof BoundLiteral literal)) {
                throw new SqlOptimizationException(
                        "predicate analysis requires canonical column-literal comparison");
            }
            ColumnConstraint constraint = constraints.computeIfAbsent(
                    column.columnOrdinal(), ignored -> new ColumnConstraint());
            // 3、Binder 当前提前拒绝重复 equality；此处仍防御其它 compiler/optimizer 实现。
            if (comparison.operator() == BoundComparisonOperator.EQUAL) {
                if (equalities.putIfAbsent(
                        column.columnOrdinal(), literal.value()) != null) {
                    throw new SqlOptimizationException(
                            "duplicate equality predicate for column ordinal "
                                    + column.columnOrdinal());
                }
            } else {
                allEqualities = false;
            }
            empty |= constraint.add(
                    comparison.operator(), literal.value());
        }
        // 4、只发布不可变分析快照，原 PredicateSet 从未被修改。
        return new PredicateAnalysis(
                Map.copyOf(constraints), Map.copyOf(equalities),
                allEqualities, empty);
    }

    /**
     * 单列 comparison 的安全交集。
     */
    public static final class ColumnConstraint {
        /** 非 NULL equality；SQL NULL 只产生 empty 证明。 */
        private SqlValue equality;
        /** 当前可证明的最严格 SQL 下界。 */
        private BoundValue lower;
        /** 当前可证明的最严格 SQL 上界。 */
        private BoundValue upper;

        /**
         * 判断是否存在非 NULL equality。
         *
         * @return 已记录等值约束时为 {@code true}
         */
        public boolean hasEquality() {
            return equality != null;
        }

        /**
         * 返回非 NULL equality。
         *
         * @return 已记录的 typed SQL 值；{@link #hasEquality()} 为 false 时返回
         *         {@code null}
         */
        public SqlValue equality() {
            return equality;
        }

        /**
         * 返回当前可安全下沉的最严格下界。
         *
         * @return 下界及开放/闭合属性；没有可证明下界时返回 {@code null}
         */
        public BoundValue lower() {
            return lower;
        }

        /**
         * 返回当前可安全下沉的最严格上界。
         *
         * @return 上界及开放/闭合属性；没有可证明上界时返回 {@code null}
         */
        public BoundValue upper() {
            return upper;
        }

        private boolean add(
                BoundComparisonOperator operator, SqlValue value) {
            // 1、普通 comparison 与 SQL NULL 结果为 UNKNOWN，不能形成 B+Tree endpoint。
            if (value instanceof SqlValue.NullValue) {
                return true;
            }
            // 2、只使用 compareKnown 能证明的顺序收紧上下界。
            switch (operator) {
                case EQUAL -> equality = value;
                case GREATER_THAN ->
                        lower = stricterLower(
                                lower, new BoundValue(value, false));
                case GREATER_THAN_OR_EQUAL ->
                        lower = stricterLower(
                                lower, new BoundValue(value, true));
                case LESS_THAN ->
                        upper = stricterUpper(
                                upper, new BoundValue(value, false));
                case LESS_THAN_OR_EQUAL ->
                        upper = stricterUpper(
                                upper, new BoundValue(value, true));
            }
            // 3、每次合并后重新证明交集是否仍可能为 TRUE。
            return contradictory();
        }

        private boolean contradictory() {
            if (equality != null) {
                Integer lowerOrder =
                        lower == null ? null
                                : compareKnown(equality, lower.value());
                if (lowerOrder != null && (lowerOrder < 0
                        || lowerOrder == 0 && !lower.inclusive())) {
                    return true;
                }
                Integer upperOrder =
                        upper == null ? null
                                : compareKnown(equality, upper.value());
                if (upperOrder != null && (upperOrder > 0
                        || upperOrder == 0 && !upper.inclusive())) {
                    return true;
                }
            }
            if (lower != null && upper != null) {
                Integer order =
                        compareKnown(lower.value(), upper.value());
                return order != null && (order > 0
                        || order == 0
                        && (!lower.inclusive() || !upper.inclusive()));
            }
            return false;
        }

        private static BoundValue stricterLower(
                BoundValue current, BoundValue candidate) {
            if (current == null) {
                return candidate;
            }
            Integer order =
                    compareKnown(candidate.value(), current.value());
            if (order == null || order < 0) {
                return current;
            }
            if (order > 0) {
                return candidate;
            }
            return new BoundValue(
                    current.value(),
                    current.inclusive() && candidate.inclusive());
        }

        private static BoundValue stricterUpper(
                BoundValue current, BoundValue candidate) {
            if (current == null) {
                return candidate;
            }
            Integer order =
                    compareKnown(candidate.value(), current.value());
            if (order == null || order > 0) {
                return current;
            }
            if (order < 0) {
                return candidate;
            }
            return new BoundValue(
                    current.value(),
                    current.inclusive() && candidate.inclusive());
        }
    }

    /**
     * 只比较无需 storage charset/collation 的同类 typed value。
     */
    private static Integer compareKnown(
            SqlValue left, SqlValue right) {
        if (left.equals(right)) {
            return 0;
        }
        if (left instanceof SqlValue.IntegerValue a
                && right instanceof SqlValue.IntegerValue b) {
            return a.value().compareTo(b.value());
        }
        if (left instanceof SqlValue.FloatingValue a
                && right instanceof SqlValue.FloatingValue b) {
            return Double.compare(a.value(), b.value());
        }
        if (left instanceof SqlValue.DecimalValue a
                && right instanceof SqlValue.DecimalValue b) {
            return a.value().compareTo(b.value());
        }
        if (left instanceof SqlValue.TemporalValue a
                && right instanceof SqlValue.TemporalValue b
                && a.kind() == b.kind()) {
            return Long.compare(a.value(), b.value());
        }
        if (left instanceof SqlValue.EnumValue a
                && right instanceof SqlValue.EnumValue b) {
            return Integer.compare(a.ordinal(), b.ordinal());
        }
        if (left instanceof SqlValue.SetValue a
                && right instanceof SqlValue.SetValue b) {
            return Long.compareUnsigned(a.bitmap(), b.bitmap());
        }
        if (left instanceof SqlValue.BitValue a
                && right instanceof SqlValue.BitValue b
                && a.bitWidth() == b.bitWidth()) {
            byte[] leftBytes = a.bytes();
            byte[] rightBytes = b.bytes();
            for (int index = 0; index < leftBytes.length; index++) {
                int order = Integer.compare(
                        Byte.toUnsignedInt(leftBytes[index]),
                        Byte.toUnsignedInt(rightBytes[index]));
                if (order != 0) {
                    return order;
                }
            }
            return 0;
        }
        return null;
    }

    /**
     * 列约束的一侧 SQL 边界。
     *
     * @param value exact column type 的非 NULL 值
     * @param inclusive comparison 是否包含该值
     */
    public record BoundValue(SqlValue value, boolean inclusive) {
        /**
         * 校验 range 边界值，SQL NULL 不允许伪装成可下沉 endpoint。
         *
         * @throws SqlOptimizationException value 缺失或为 SQL NULL 时抛出
         */
        public BoundValue {
            if (value == null || value instanceof SqlValue.NullValue) {
                throw new SqlOptimizationException(
                        "optimizer range bound requires a non-null SQL value");
            }
        }
    }

    /**
     * 一次 predicate conjunction 的冻结分析结果。
     *
     * @param constraints ordinal 到安全列约束的不可变映射
     * @param equalities ordinal 到 equality 值的不可变映射
     * @param allEqualities 全部 residual 是否都是 equality comparison
     * @param empty 是否已证明 WHERE 不可能为 TRUE
     */
    public record PredicateAnalysis(
            Map<Integer, ColumnConstraint> constraints,
            Map<Integer, SqlValue> equalities,
            boolean allEqualities,
            boolean empty) {
        public PredicateAnalysis {
            if (constraints == null || equalities == null
                    || constraints.entrySet().stream().anyMatch(entry ->
                    entry.getKey() == null || entry.getKey() < 0
                            || entry.getValue() == null)
                    || equalities.entrySet().stream().anyMatch(entry ->
                    entry.getKey() == null || entry.getKey() < 0
                            || entry.getValue() == null)) {
                throw new SqlOptimizationException(
                        "predicate analysis maps contain invalid entries");
            }
            constraints = Map.copyOf(constraints);
            equalities = Map.copyOf(equalities);
        }

        /**
         * 仅在全部 residual 都为 equality 时返回映射。
         *
         * @return exact equality map；含 comparison range 或 truth literal 时为空
         */
        public Optional<Map<Integer, SqlValue>> exactEqualities() {
            return allEqualities
                    ? Optional.of(equalities) : Optional.empty();
        }
    }
}
