package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.HashSet;
import java.util.List;

/**
 * 单表 UPDATE 的纯语义绑定结果。谓词仍是 SQL 真值权威，是否可降为主键点修改由 Optimizer 决定。
 *
 * @param table statement metadata lease 固定的 exact table version
 * @param assignmentOrdinals 按 table ordinal 严格升序排列的非主键赋值列
 * @param assignmentValues 与 ordinal 一一对应的 typed values
 * @param condition 完成名称解析和类型转换的完整 WHERE condition
 */
public record BoundUpdate(TableDefinition table, List<Integer> assignmentOrdinals,
                          List<SqlValue> assignmentValues,
                          BoundExpression condition)
        implements BoundRelationalStatement {

    /**
     * 冻结 UPDATE 语义并拒绝重复、乱序或主键赋值。
     *
     * <ol>
     *     <li>校验 table、assignment/value 一一对应关系与完整 condition。</li>
     *     <li>从 exact primary index 建立稳定 column-id 禁止集，并验证 ordinal
     *         严格升序、唯一、范围合法且不修改主键。</li>
     *     <li>递归核对 condition 的列身份和 exact DD type。</li>
     *     <li>冻结 assignment 集合，失败路径不发布部分 update 语义。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 字段缺失、assignment 非法或 predicate 越界时抛出
     */
    public BoundUpdate {
        // 1、assignment 与 condition 必须作为一个完整 semantic unit 构造。
        if (table == null || assignmentOrdinals == null || assignmentValues == null
                || condition == null || assignmentOrdinals.isEmpty()
                || assignmentOrdinals.size() != assignmentValues.size()
                || assignmentValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid semantic bound UPDATE fields");
        }
        // 2、用 column id 保护聚簇身份，ordinal 只表达当前 exact version 的位置。
        HashSet<Integer> unique = new HashSet<>();
        HashSet<Long> primaryColumnIds = new HashSet<>();
        table.primaryIndex().keyParts().forEach(part -> primaryColumnIds.add(part.columnId()));
        int previous = -1;
        for (Integer ordinal : assignmentOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || ordinal <= previous || !unique.add(ordinal)
                    || primaryColumnIds.contains(table.columns().get(ordinal).columnId())) {
                throw new DatabaseValidationException(
                        "semantic UPDATE ordinals must be unique ascending non-primary columns");
            }
            previous = ordinal;
        }
        // 3、最终 WHERE truth 不得引用其它 metadata version 的列位置或类型。
        BoundExpressionValidation.validateCondition(condition, table);
        // 4、执行前任何调用方修改都不能改变 assignment 语义。
        assignmentOrdinals = List.copyOf(assignmentOrdinals);
        assignmentValues = List.copyOf(assignmentValues);
    }
}
