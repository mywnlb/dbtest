package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * 二表 INNER JOIN 的纯语义绑定结果。列位置使用左表全部列后拼接右表全部列的稳定
 * statement schema；访问顺序、索引 probe 和运行期 cursor 不属于 Binder。
 *
 * @param tables SQL 顺序排列的两个 exact table version
 * @param projectionOrdinals 用户投影在扁平 statement schema 中的位置
 * @param joinCondition 完成名称和 exact type 绑定的列列等值 ON 条件
 * @param condition 独立保存的完整 WHERE residual
 * @param orderBy 扁平 statement schema 上的排序键
 * @param limit 规范化 offset/count
 * @param lockMode v1 只允许一致性读
 */
public record BoundJoinSelect(
        List<TableDefinition> tables,
        List<Integer> projectionOrdinals,
        BoundExpression joinCondition,
        BoundExpression condition,
        List<BoundSortKey> orderBy,
        Optional<BoundLimit> limit,
        SelectLockMode lockMode) implements BoundRelationalStatement {

    /**
     * 冻结二表语义并交叉校验所有 stable column identity。
     *
     * <ol>
     *     <li>要求恰好两个表和一致性读，防止未实现的多表/locking 语义进入计划。</li>
     *     <li>按左宽度加右宽度验证投影位置和唯一性。</li>
     *     <li>分别校验 ON/WHERE 中的 relation ordinal、local ordinal、column id 与 exact type。</li>
     *     <li>校验扁平排序键身份并冻结全部集合；失败不创建逻辑计划或执行资源。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 任一字段、列身份或读取模式不满足 v1 契约时抛出
     */
    public BoundJoinSelect {
        // 1、v1 的语义边界必须在 Binder 输出构造时闭合。
        if (tables == null || tables.size() != 2
                || tables.stream().anyMatch(java.util.Objects::isNull)
                || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || joinCondition == null || condition == null
                || orderBy == null || limit == null
                || lockMode != SelectLockMode.CONSISTENT) {
            throw new DatabaseValidationException(
                    "invalid bound two-table INNER JOIN fields");
        }
        int width = tables.stream()
                .mapToInt(table -> table.columns().size()).sum();
        // 2、扁平位置是后续 Logical/Physical/Executor 之间的稳定 schema 协议。
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= width
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException(
                        "bound JOIN projection ordinal is invalid or duplicate");
            }
        }
        // 3、列引用仍保留 local ordinal，避免扁平位置掩盖 exact table version 漂移。
        BoundExpressionValidation.validateCondition(joinCondition, tables);
        BoundExpressionValidation.validateCondition(condition, tables);
        // 4、排序位置使用扁平 schema，同时以 stable id 校验真实 DD 列。
        for (BoundSortKey key : orderBy) {
            if (key == null || key.columnOrdinal() >= width
                    || flattenedColumnId(tables, key.columnOrdinal())
                    != key.columnId()) {
                throw new DatabaseValidationException(
                        "bound JOIN ORDER BY key is outside exact statement schema");
            }
        }
        tables = List.copyOf(tables);
        projectionOrdinals = List.copyOf(projectionOrdinals);
        orderBy = List.copyOf(orderBy);
    }

    private static long flattenedColumnId(
            List<TableDefinition> tables, int ordinal) {
        int remaining = ordinal;
        for (TableDefinition table : tables) {
            if (remaining < table.columns().size()) {
                return table.columns().get(remaining).columnId();
            }
            remaining -= table.columns().size();
        }
        throw new DatabaseValidationException(
                "flattened JOIN column ordinal exceeds statement schema");
    }
}
