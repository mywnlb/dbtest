package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundInsert;
import cn.zhangyis.db.sql.binder.bound.BoundJoinSelect;
import cn.zhangyis.db.sql.binder.bound.BoundRelationalStatement;
import cn.zhangyis.db.sql.binder.bound.BoundSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;

import java.util.List;

/**
 * 把纯语义 Bound IR 转换为无访问路径的关系树。转换器无共享状态，不读取 DD repository，也不执行规则。
 */
public final class BoundToLogicalConverter {

    /**
     * 构建当前 scan/filter/project/values/table-modify 关系树。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 semantic Bound 输入；sealed hierarchy 让新增语句必须显式补齐转换分支。</li>
     *     <li>SELECT 构造 project(filter(scan))，并把不可改写的 read intent 保存在 scan。</li>
     *     <li>INSERT 构造 modify(values)，UPDATE/DELETE 构造 modify(filter(current-scan))。</li>
     *     <li>返回不可变根；本阶段不选择 index、range，不获取新的 metadata 或执行期资源。</li>
     * </ol>
     *
     * @param statement Binder 产生且仍受 statement/transaction metadata lease 保护的语义语句
     * @return 不含物理访问路径的不可变逻辑计划
     * @throws DatabaseValidationException 输入缺失或不是当前 semantic statement 时抛出
     */
    public LogicalPlan convert(BoundRelationalStatement statement) {
        // 1、空输入在构造逻辑节点前失败；sealed switch 负责保持转换分支穷尽。
        if (statement == null) {
            throw new DatabaseValidationException("bound relational statement must not be null");
        }
        LogicalPlan result = switch (statement) {
            // 2、read mode 保存在 scan，后续规则不得把 consistent read 改成 locking read，反之亦然。
            case BoundSelect select -> {
                RelNode scan = new LogicalTableScan(select.table(), select.lockMode());
                RelNode filter = new LogicalFilter(
                        scan, PredicateSet.of(select.condition()));
                yield new LogicalPlan(new LogicalProject(
                        filter, select.projectionOrdinals(),
                        select.orderBy(), select.limit()));
            }
            case BoundJoinSelect select -> {
                RelNode left = new LogicalTableScan(
                        select.tables().getFirst(),
                        SelectLockMode.CONSISTENT);
                RelNode right = new LogicalTableScan(
                        select.tables().getLast(),
                        SelectLockMode.CONSISTENT);
                RelNode join = new LogicalJoin(
                        left, right,
                        PredicateSet.of(select.joinCondition()));
                RelNode filter = new LogicalFilter(
                        join, PredicateSet.of(select.condition()));
                yield new LogicalPlan(new LogicalProject(
                        filter, select.projectionOrdinals(),
                        select.orderBy(), select.limit()));
            }
            // 3、INSERT values 不经过 scan；UPDATE/DELETE 强制 current-read scan。
            case BoundInsert insert -> new LogicalPlan(new LogicalTableModify(
                    insert.table(), ModificationKind.INSERT,
                    new LogicalValues(insert.table(), insert.batch()), List.of(), List.of()));
            case BoundUpdate update -> {
                RelNode scan = new LogicalTableScan(update.table(), SelectLockMode.FOR_UPDATE);
                RelNode filter = new LogicalFilter(
                        scan, PredicateSet.of(update.condition()));
                yield new LogicalPlan(new LogicalTableModify(
                        update.table(), ModificationKind.UPDATE, filter,
                        update.assignmentOrdinals(), update.assignmentValues()));
            }
            case BoundDelete delete -> {
                RelNode scan = new LogicalTableScan(delete.table(), SelectLockMode.FOR_UPDATE);
                RelNode filter = new LogicalFilter(
                        scan, PredicateSet.of(delete.condition()));
                yield new LogicalPlan(new LogicalTableModify(
                        delete.table(), ModificationKind.DELETE, filter, List.of(), List.of()));
            }
        };
        // 4、完整逻辑根一次返回；转换期间未获取新的 metadata、事务或存储资源。
        return result;
    }
}
