package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundExpressionValidation;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * 单表 SELECT 的纯语义绑定结果，不包含 access index、range 或 point/range 分类。
 *
 * @param table statement metadata lease 固定的 exact table version
 * @param projectionOrdinals 用户投影的 table column ordinal，顺序即公开结果顺序
 * @param condition 完成列解析和类型转换的 boolean condition；Optimizer 不得删除最终 residual
 * @param orderBy 完成名称解析的稳定排序键；允许引用未投影列
 * @param limit 规范化后的 offset/count；空表示不限制
 * @param lockMode 一致性读或当前版本锁定读语义；规则改写不得改变该属性
 */
public record BoundSelect(TableDefinition table, List<Integer> projectionOrdinals,
                          BoundExpression condition, List<BoundSortKey> orderBy,
                          Optional<BoundLimit> limit, SelectLockMode lockMode)
        implements BoundRelationalStatement {

    /**
     * 校验并冻结 SELECT 语义。
     *
     * <ol>
     *     <li>拒绝缺失 table、projection、condition 或 read intent。</li>
     *     <li>逐项校验投影 ordinal 的范围与唯一性，保持用户可观察顺序。</li>
     *     <li>递归核对 condition 的列 id、ordinal、exact DD type 与当前 table version。</li>
     *     <li>冻结投影集合；表达式节点自身不可变，无需复制第二份谓词状态。</li>
     * </ol>
     *
     * @throws DatabaseValidationException table、投影、谓词或读取模式无效时抛出
     */
    public BoundSelect {
        // 1、任何不完整 semantic IR 都必须早于 logical plan 构造失败。
        if (table == null || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || condition == null || orderBy == null || limit == null || lockMode == null) {
            throw new DatabaseValidationException("invalid semantic bound SELECT fields");
        }
        // 2、projection 顺序进入公开结果，但重复/越界位置没有合法 SQL 含义。
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException(
                        "semantic SELECT projection ordinal is invalid or duplicate");
            }
        }
        // 3、stable column identity 与 exact table version 必须在 Binder 边界闭合。
        BoundExpressionValidation.validateCondition(condition, table);
        for (BoundSortKey key : orderBy) {
            if (key == null || key.columnOrdinal() >= table.columns().size()
                    || table.columns().get(key.columnOrdinal()).columnId() != key.columnId()) {
                throw new DatabaseValidationException(
                        "bound ORDER BY key does not belong to exact table version");
            }
        }
        // 4、调用方不能在规则或 Executor 运行期间修改 projection/order。
        projectionOrdinals = List.copyOf(projectionOrdinals);
        orderBy = List.copyOf(orderBy);
    }

    /**
     * 保留排序能力引入前的构造形状。
     */
    public BoundSelect(
            TableDefinition table, List<Integer> projectionOrdinals,
            BoundExpression condition, SelectLockMode lockMode) {
        this(table, projectionOrdinals, condition,
                List.of(), Optional.empty(), lockMode);
    }
}
