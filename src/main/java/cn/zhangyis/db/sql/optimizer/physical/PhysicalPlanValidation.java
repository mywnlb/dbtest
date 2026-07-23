package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundTruthLiteral;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.HashSet;
import java.util.List;

/**
 * 物理计划跨字段不变量的无状态校验器。所有失败发生在 Executor 创建事务或存储资源之前。
 */
final class PhysicalPlanValidation {
    private PhysicalPlanValidation() {
    }

    /**
     * 在计划绑定的 exact table version 中解析稳定索引身份。
     *
     * @param table metadata lease 固定的表定义
     * @param accessIndexId optimizer 选择的稳定 index id
     * @return 属于同一 table version 的索引定义
     * @throws DatabaseValidationException table 缺失或索引 id 不属于该版本时抛出
     */
    static IndexDefinition requireIndex(TableDefinition table, long accessIndexId) {
        if (table == null) {
            throw new DatabaseValidationException("physical plan table must not be null");
        }
        return table.indexes().stream()
                .filter(candidate -> candidate.id().value() == accessIndexId)
                .findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "physical plan references missing DD index: " + accessIndexId));
    }

    /**
     * 交叉校验通用 range 计划的索引、endpoint 与最终 residual。
     *
     * <ol>
     *     <li>校验 range/residual 容器，阻止不完整计划进入 Executor。</li>
     *     <li>在 exact table version 中解析索引并按 key width 校验两侧 endpoint。</li>
     *     <li>复核每个 residual ordinal，确保最终真值不会读取其它 table version 的列。</li>
     * </ol>
     *
     * @param table 计划绑定的 exact table version
     * @param accessIndexId 候选扫描索引的稳定 id
     * @param range 可无界或使用连续 key prefix 的物理范围
     * @param predicates 不能由访问范围替代的完整 typed residual
     * @throws DatabaseValidationException 任一字段、索引、endpoint 或 predicate 与 table 不一致时抛出
     */
    static void validateRange(TableDefinition table, long accessIndexId, IndexRange range,
                              PredicateSet predicates) {
        // 1、空 residual 不属于当前 Parser/Binder 支持的单表 WHERE 形状。
        if (range == null || predicates == null) {
            throw new DatabaseValidationException("invalid physical range plan fields");
        }
        // 2、endpoint 可以短于完整 key 形成 prefix range，但绝不能越过 index key width。
        IndexDefinition index = requireIndex(table, accessIndexId);
        range.lower().ifPresent(endpoint -> validateEndpoint(endpoint, index));
        range.upper().ifPresent(endpoint -> validateEndpoint(endpoint, index));
        // 3、residual ordinal 始终相对同一个 exact table version 解释。
        validateCanonicalResidual(table, predicates);
    }

    /**
     * 校验物理计划 residual 已经过规则规范化，且仍绑定同一个 exact table version。
     *
     * <ol>
     *     <li>递归核对列 id、ordinal 和完整 DD 类型，阻止 metadata version 漂移。</li>
     *     <li>递归要求 comparison 为 column-literal、null-test 为 column/literal，
     *         使 Data Port 不必猜测未规范 scalar 方向。</li>
     * </ol>
     *
     * @param table 计划绑定的 exact table version
     * @param predicates RuleProgram 输出且 condition 为唯一权威的完整 residual
     * @throws DatabaseValidationException residual 引用错误 table version 或未规范时抛出
     */
    static void validateCanonicalResidual(
            TableDefinition table, PredicateSet predicates) {
        // 1、表达式中的稳定列身份必须与物理计划的 metadata lease 完全一致。
        if (predicates == null) {
            throw new DatabaseValidationException(
                    "physical residual must not be null");
        }
        BoundExpressionValidation.validateCondition(
                predicates.condition(), table);
        // 2、当前 Data Port evaluator 只消费规则固定点后的 M3 canonical expression。
        validateExecutableResidual(predicates.condition());
    }

    /**
     * 递归校验 M3 residual 的可执行形状；boolean 组合不改变 scalar 规范要求。
     *
     * @param expression 当前 residual 节点
     * @throws DatabaseValidationException comparison/null-test 未规范或 scalar 作为 boolean 根时抛出
     */
    private static void validateExecutableResidual(
            BoundExpression expression) {
        switch (expression) {
            case BoundTruthLiteral ignored -> {
                // 三值常量可直接由 evaluator 消费。
            }
            case BoundComparison comparison -> {
                if (!(comparison.left()
                        instanceof BoundColumnReference)
                        || !(comparison.right()
                        instanceof BoundLiteral)) {
                    throw new DatabaseValidationException(
                            "physical residual requires canonical column-literal comparisons");
                }
            }
            case BoundConjunction conjunction ->
                    conjunction.operands().forEach(
                            PhysicalPlanValidation::validateExecutableResidual);
            case BoundDisjunction disjunction ->
                    disjunction.operands().forEach(
                            PhysicalPlanValidation::validateExecutableResidual);
            case BoundNegation negation ->
                    validateExecutableResidual(negation.operand());
            case BoundNullTest nullTest -> {
                if (!(nullTest.operand()
                        instanceof BoundColumnReference)
                        && !(nullTest.operand()
                        instanceof BoundLiteral)) {
                    throw new DatabaseValidationException(
                            "physical null-test requires a column or literal operand");
                }
            }
            case BoundColumnReference ignored ->
                    throw new DatabaseValidationException(
                            "physical residual contains scalar column root");
            case BoundLiteral ignored ->
                    throw new DatabaseValidationException(
                            "physical residual contains scalar literal root");
        }
    }

    /**
     * 证明 point access key 是完整 residual 的安全必要条件，防止替代 optimizer
     * 构造会漏行的 under-scan 计划。
     *
     * <ol>
     *     <li>先校验 canonical residual 与完整 key 宽度。</li>
     *     <li>按 index key part 反查稳定 column id，并要求 residual 含相同 typed equality。</li>
     * </ol>
     *
     * @param table 计划绑定的 exact table version
     * @param index point access 使用的无 prefix 唯一/普通二级索引
     * @param keyValues 按 index key-part 顺序排列的完整访问 key
     * @param predicates 最终 SQL 真值权威
     * @throws DatabaseValidationException access key 不能由 residual 中相同 equality
     *         证明时抛出
     */
    static void validatePointResidual(
            TableDefinition table, IndexDefinition index,
            List<SqlValue> keyValues, PredicateSet predicates) {
        // 1、调用方不得以 key 容器和 residual 的两份不一致状态进入 Executor。
        validateCanonicalResidual(table, predicates);
        if (keyValues.size() != index.keyParts().size()) {
            throw new DatabaseValidationException(
                    "physical point residual key width mismatch");
        }
        // 2、每个访问 key 都必须来自完整 WHERE 的同列等值条件；额外 residual 仍保留。
        for (int keyOrdinal = 0;
             keyOrdinal < index.keyParts().size(); keyOrdinal++) {
            long columnId =
                    index.keyParts().get(keyOrdinal).columnId();
            SqlValue keyValue = keyValues.get(keyOrdinal);
            boolean proven = predicates.conjuncts().stream()
                    .filter(BoundComparison.class::isInstance)
                    .map(BoundComparison.class::cast)
                    .anyMatch(comparison ->
                            comparison.operator()
                                    == BoundComparisonOperator.EQUAL
                                    && ((BoundColumnReference)
                                    comparison.left()).columnId()
                                    == columnId
                                    && ((BoundLiteral)
                                    comparison.right()).value()
                                    .equals(keyValue));
            if (!proven) {
                throw new DatabaseValidationException(
                        "physical point key is not proven by complete residual");
            }
        }
    }

    /**
     * 校验 SELECT 公开投影的范围、唯一性与顺序容器。
     *
     * @param table exact table version
     * @param projections 保持用户顺序的列 ordinal
     * @throws DatabaseValidationException 投影缺失、重复或越界时抛出
     */
    static void validateProjection(TableDefinition table, List<Integer> projections) {
        if (table == null || projections == null || projections.isEmpty()) {
            throw new DatabaseValidationException(
                    "physical SELECT projection must not be empty");
        }
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projections) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException(
                        "physical SELECT projection is invalid or duplicate");
            }
        }
    }

    /**
     * 校验 point DML 的完整、非 NULL、无 prefix 聚簇主键。
     *
     * <ol>
     *     <li>复核 table/key 容器、SQL NULL、key width 与 primary prefix 属性。</li>
     *     <li>拒绝当前 record key pipeline 不支持的 LOB/JSON 主键。</li>
     * </ol>
     *
     * @param table exact table version
     * @param values 按 primary key-part 顺序排列的定位值
     * @param operation 用于稳定诊断的 UPDATE/DELETE 操作名
     * @throws DatabaseValidationException key 不能作为唯一聚簇记录身份时抛出
     */
    static void validatePrimaryKey(
            TableDefinition table, List<SqlValue> values, String operation) {
        // 1、point 写必须精确定位一条真实记录，SQL NULL 和 prefix key 都不能作为身份。
        if (table == null || values == null
                || values.stream().anyMatch(java.util.Objects::isNull)
                || values.stream().anyMatch(SqlValue.NullValue.class::isInstance)
                || values.size() != table.primaryIndex().keyParts().size()
                || table.primaryIndex().keyParts().stream()
                .anyMatch(part -> part.prefixBytes() != 0)) {
            throw new DatabaseValidationException(
                    "invalid physical " + operation + " primary key");
        }
        // 2、LOB/JSON 外置值不进入当前 B+Tree point-key codec。
        rejectLobKey(table, table.primaryIndex());
    }

    /**
     * 校验 UPDATE patch 的一一对应、升序和非主键不变量。
     *
     * <ol>
     *     <li>验证容器完整且 typed value 不含 Java {@code null}。</li>
     *     <li>从 exact primary index 形成禁止赋值的 column-id 集。</li>
     *     <li>逐 ordinal 验证范围、严格升序和非主键属性。</li>
     * </ol>
     *
     * @param table exact table version
     * @param ordinals table column ordinal patch
     * @param values 与 ordinals 一一对应的 typed values
     * @param operation 用于稳定诊断的 point/range UPDATE 名称
     * @throws DatabaseValidationException patch 缺失、乱序、越界或修改主键时抛出
     */
    static void validateAssignments(
            TableDefinition table, List<Integer> ordinals, List<SqlValue> values,
            String operation) {
        // 1、容器错误早于 DD key-part 解析。
        if (table == null || ordinals == null || values == null || ordinals.isEmpty()
                || ordinals.size() != values.size()
                || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "invalid physical " + operation + " assignments");
        }
        // 2、用 column id 而不是 ordinal 固定 primary identity，抵御不同 table version ordinal 漂移。
        HashSet<Long> primaryColumnIds = new HashSet<>();
        table.primaryIndex().keyParts()
                .forEach(part -> primaryColumnIds.add(part.columnId()));
        int previous = -1;
        // 3、严格升序同时提供唯一性，Executor/Data Port 可以按 ordinal 线性构造 patch。
        for (Integer ordinal : ordinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || ordinal <= previous
                    || primaryColumnIds.contains(table.columns().get(ordinal).columnId())) {
                throw new DatabaseValidationException(
                        "physical " + operation
                                + " ordinals must be ascending non-primary columns");
            }
            previous = ordinal;
        }
    }

    /**
     * 拒绝当前 SQL key codec 不支持的 LOB/JSON index key，并验证 DD column 引用完整。
     *
     * @param table exact table version
     * @param index 属于该版本的访问索引
     * @throws DatabaseValidationException index 引用缺列或包含 LOB/JSON key 时抛出
     */
    static void rejectLobKey(TableDefinition table, IndexDefinition index) {
        for (var part : index.keyParts()) {
            var column = table.columns().stream()
                    .filter(candidate -> candidate.columnId() == part.columnId())
                    .findFirst()
                    .orElseThrow(() -> new DatabaseValidationException(
                            "physical index references missing DD column"));
            if (isLob(column.type().typeId())) {
                throw new DatabaseValidationException(
                        "physical plan does not support LOB/JSON index key");
            }
        }
    }

    /**
     * 验证 endpoint 是访问索引的连续前缀而非越界 key。
     *
     * @param endpoint 待检查的一侧物理边界
     * @param index endpoint 所属访问索引
     * @throws DatabaseValidationException endpoint key 数超过 index key-part 数时抛出
     */
    private static void validateEndpoint(RangeEndpoint endpoint, IndexDefinition index) {
        if (endpoint.keyValues().size() > index.keyParts().size()
                || endpoint.keyValues().stream()
                .anyMatch(SqlValue.NullValue.class::isInstance)) {
            throw new DatabaseValidationException(
                    "physical range endpoint exceeds index key width or contains SQL NULL");
        }
    }

    private static boolean isLob(DictionaryTypeId type) {
        return switch (type) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
    }
}
