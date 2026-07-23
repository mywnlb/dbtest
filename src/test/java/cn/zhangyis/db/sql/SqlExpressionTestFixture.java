package cn.zhangyis.db.sql;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SQL M2 测试表达式工厂。测试通过 exact {@link TableDefinition} 构造列引用，
 * 避免继续复制已经废弃的 ordinal/value 谓词模型。
 */
public final class SqlExpressionTestFixture {
    /** 测试直接构造 IR 时使用的稳定虚拟位置，不参与生产错误定位断言。 */
    private static final SourcePosition TEST_POSITION =
            new SourcePosition(0, 1, 1);

    private SqlExpressionTestFixture() {
    }

    /**
     * 构造与 exact table version 绑定的 column-literal comparison。
     *
     * @param table 当前测试语句绑定的不可变表定义
     * @param columnOrdinal exact table version 中的零基列位置
     * @param operator comparison 操作符
     * @param value 已按目标列类型构造的 SQL 值；SQL NULL 使用显式 NullValue
     * @return 同时携带 column id、ordinal、完整类型和源位置的 bound comparison
     * @throws DatabaseValidationException ordinal 越界或参数缺失时抛出
     */
    public static BoundComparison comparison(
            TableDefinition table,
            int columnOrdinal,
            BoundComparisonOperator operator,
            SqlValue value) {
        if (table == null || operator == null || value == null
                || columnOrdinal < 0
                || columnOrdinal >= table.columns().size()) {
            throw new DatabaseValidationException(
                    "invalid test comparison fields");
        }
        ColumnDefinition column = table.columns().get(columnOrdinal);
        return new BoundComparison(
                new BoundColumnReference(
                        column.columnId(), columnOrdinal, column.type(),
                        TEST_POSITION),
                operator,
                new BoundLiteral(value, column.type(), TEST_POSITION));
    }

    /**
     * 构造 column = literal。
     *
     * @param table 当前测试语句绑定的不可变表定义
     * @param columnOrdinal exact table version 中的零基列位置
     * @param value 已按列类型构造的 SQL 值
     * @return exact-type 等值表达式
     */
    public static BoundComparison equal(
            TableDefinition table, int columnOrdinal, SqlValue value) {
        return comparison(
                table, columnOrdinal, BoundComparisonOperator.EQUAL, value);
    }

    /**
     * 按给定顺序组合测试条件，不排序、不去重。
     *
     * @param expressions 至少一个 boolean bound expression
     * @return 单元素时原样返回，多元素时返回 conjunction
     * @throws DatabaseValidationException 没有条件或包含空元素时抛出
     */
    public static BoundExpression and(BoundExpression... expressions) {
        if (expressions == null || expressions.length == 0
                || Arrays.stream(expressions)
                .anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "test conjunction requires expressions");
        }
        if (expressions.length == 1) {
            return expressions[0];
        }
        return new BoundConjunction(List.of(expressions));
    }

    /**
     * 按给定顺序组合测试 OR 条件，不执行范围 union 或谓词重排。
     *
     * @param expressions 至少两个 boolean bound expression
     * @return 保持用户顺序的 disjunction
     */
    public static BoundDisjunction or(BoundExpression... expressions) {
        if (expressions == null || expressions.length < 2
                || Arrays.stream(expressions)
                .anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "test disjunction requires at least two expressions");
        }
        return new BoundDisjunction(List.of(expressions));
    }

    /**
     * 构造与 exact table version 绑定的列 null-test。
     *
     * @param table 当前测试语句绑定的不可变表定义
     * @param columnOrdinal exact table version 中的零基列位置
     * @param operator IS NULL 或 IS NOT NULL
     * @return 非 nullable boolean null-test
     */
    public static BoundNullTest nullTest(
            TableDefinition table, int columnOrdinal,
            BoundNullTestOperator operator) {
        if (table == null || operator == null || columnOrdinal < 0
                || columnOrdinal >= table.columns().size()) {
            throw new DatabaseValidationException(
                    "invalid test null-test fields");
        }
        ColumnDefinition column = table.columns().get(columnOrdinal);
        return new BoundNullTest(
                new BoundColumnReference(
                        column.columnId(), columnOrdinal, column.type(),
                        TEST_POSITION),
                operator, TEST_POSITION);
    }

    /**
     * 从有序条件构造物理计划使用的完整残余谓词。
     *
     * @param expressions 至少一个 boolean bound expression
     * @return condition 为唯一权威状态的 PredicateSet
     */
    public static PredicateSet predicates(BoundExpression... expressions) {
        return PredicateSet.of(and(expressions));
    }

    /**
     * 按索引 key part 顺序构造 point/range 计划的等值残余谓词。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 exact table version 定位稳定 index id，拒绝不存在的测试元数据。</li>
     *     <li>校验 key value 数量覆盖完整 logical key，防止测试制造不合法 point plan。</li>
     *     <li>把每个 key part 映射回 exact column ordinal，并按索引顺序生成等值表达式。</li>
     * </ol>
     *
     * @param table 当前测试语句绑定的不可变表定义
     * @param indexId point/range 访问使用的稳定索引 id
     * @param keyValues 按 logical index key part 排列的完整 SQL key
     * @return 保留完整 SQL 等值语义的残余 PredicateSet
     * @throws DatabaseValidationException 索引不存在、key 数量不匹配或 key part
     *         不能映射到 exact table column 时抛出
     */
    public static PredicateSet indexEqualityPredicates(
            TableDefinition table, long indexId, List<SqlValue> keyValues) {
        // 1. 只允许 exact table version 中真实存在的稳定索引身份。
        IndexDefinition index = table.indexes().stream()
                .filter(candidate -> candidate.id().value() == indexId)
                .findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "test index not found: " + indexId));

        // 2. Point residual 必须覆盖完整 logical key，不能让测试掩盖计划不变量。
        if (keyValues == null
                || keyValues.size() != index.keyParts().size()) {
            throw new DatabaseValidationException(
                    "test point key does not match index arity");
        }

        // 3. column id 经 exact table definition 反查 ordinal 后构造 typed equality。
        ArrayList<BoundExpression> equalities =
                new ArrayList<>(keyValues.size());
        for (int keyOrdinal = 0;
             keyOrdinal < index.keyParts().size(); keyOrdinal++) {
            long columnId = index.keyParts().get(keyOrdinal).columnId();
            int columnOrdinal = -1;
            for (int candidate = 0;
                 candidate < table.columns().size(); candidate++) {
                if (table.columns().get(candidate).columnId() == columnId) {
                    columnOrdinal = candidate;
                    break;
                }
            }
            if (columnOrdinal < 0) {
                throw new DatabaseValidationException(
                        "test index key column not found: " + columnId);
            }
            equalities.add(equal(
                    table, columnOrdinal, keyValues.get(keyOrdinal)));
        }
        return PredicateSet.of(and(
                equalities.toArray(BoundExpression[]::new)));
    }
}
