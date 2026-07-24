package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;

import java.util.List;

/**
 * Executor pull tree 的运行期算子。PhysicalPlan 不保存该对象；每次 statement 都创建一棵新树。
 */
public interface PlanNode extends AutoCloseable {

    /**
     * 按 child-first 顺序打开节点及其 cursor。
     *
     * @param context 当前语句私有事务能力和绝对 deadline
     */
    void open(ExecutionContext context);

    /**
     * 拉取下一行；返回前不得持有任何 storage page latch/fix。
     *
     * @return 有当前行时为 {@code true}，EOF 时为 {@code false}
     */
    boolean advance();

    /**
     * 返回最近一次成功 advance 的当前行。
     *
     * @return 只在下一次 advance/close 前有效的行视图
     */
    SqlRowView current();

    /**
     * 返回本节点输出列元数据。
     *
     * @return 与 current view width 一致的不可变列列表
     */
    List<ResultColumn> columns();

    /**
     * 返回当前显式生命周期状态。
     *
     * @return NEW/OPEN/EXHAUSTED/CLOSED
     */
    PlanNodeState state();

    /** 幂等逆序关闭 child/cursor。 */
    @Override
    void close();
}
