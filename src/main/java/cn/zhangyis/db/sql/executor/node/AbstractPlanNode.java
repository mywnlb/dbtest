package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;

/**
 * 固定 PlanNode 的 open/advance/current/close 状态机。子类只实现资源动作，不能绕过本模板
 * 发布半打开节点或在 EOF 后继续读取 cursor。
 */
abstract class AbstractPlanNode implements PlanNode {
    /** 当前节点唯一权威状态；PlanNode 只由执行 statement 线程拥有。 */
    private PlanNodeState state = PlanNodeState.NEW;
    /** 最近一次成功 advance 的视图；EOF/close 时清除。 */
    private SqlRowView current;

    /**
     * 从 NEW 打开当前节点，并在部分打开失败时立即收口子资源。
     *
     * <ol>
     *     <li>检查唯一合法前态 NEW，避免重复打开同一 statement-private 节点。</li>
     *     <li>调用子类 openNode；完整成功后才发布 OPEN，因此外部看不到半打开状态。</li>
     *     <li>失败时调用 closeNode，并把清理失败挂到主异常后发布 CLOSED。</li>
     * </ol>
     *
     * @param context 当前语句唯一事务能力与绝对 deadline；不得为 {@code null}
     * @throws SqlExecutionException 当前状态不是 NEW 时抛出，调用方不得重用该节点
     */
    @Override
    public final void open(ExecutionContext context) {
        // 1、状态校验早于任何 child/cursor 副作用。
        if (context == null || state != PlanNodeState.NEW) {
            throw new SqlExecutionException(
                    "plan node requires context and can only open from NEW: "
                            + state);
        }
        RuntimeException failure = null;
        try {
            // 2、只有完整打开 child/cursor 后才对调用方发布 OPEN。
            openNode(context);
            state = PlanNodeState.OPEN;
        } catch (RuntimeException openFailure) {
            failure = openFailure;
            throw openFailure;
        } finally {
            // 3、失败路径与正常 close 使用同一清理钩子，且不覆盖 open 根因。
            if (failure != null) {
                try {
                    closeNode();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                state = PlanNodeState.CLOSED;
            }
        }
    }

    /**
     * 拉取下一行并维护 current/EOF 状态。
     *
     * <ol>
     *     <li>EXHAUSTED 重复调用稳定返回 false，其它非 OPEN 状态拒绝。</li>
     *     <li>先清除旧视图，再委托 advanceNode，保证失败后旧行也不可继续读取。</li>
     *     <li>Java null 只在节点内部表示 EOF；非空视图成为唯一 current。</li>
     * </ol>
     *
     * @return 成功发布新 current 时为 {@code true}，EOF 时为 {@code false}
     * @throws SqlExecutionException 节点未打开、已关闭或协议状态非法时抛出
     */
    @Override
    public final boolean advance() {
        // 1、EOF 是可重复观察的终态，但仍需显式 close 才释放资源。
        if (state == PlanNodeState.EXHAUSTED) {
            return false;
        }
        if (state != PlanNodeState.OPEN) {
            throw new SqlExecutionException(
                    "plan node advance requires OPEN state: " + state);
        }
        // 2、下游失败时不得让调用方继续读上一行。
        current = null;
        SqlRowView next = advanceNode();
        // 3、只把合法视图发布到 OPEN current；null 转换为 EXHAUSTED。
        if (next == null) {
            state = PlanNodeState.EXHAUSTED;
            return false;
        }
        current = next;
        return true;
    }

    /**
     * 返回最近一次成功 advance 的行；不延长底层 cursor-owned 视图生命周期。
     *
     * @return 当前有效行视图
     * @throws SqlExecutionException 尚未成功 advance、已 EOF 或已关闭时抛出
     */
    @Override
    public final SqlRowView current() {
        if (state != PlanNodeState.OPEN || current == null) {
            throw new SqlExecutionException(
                    "plan node current row is unavailable: " + state);
        }
        return current;
    }

    @Override
    public final PlanNodeState state() {
        return state;
    }

    /**
     * 幂等关闭当前节点。
     *
     * <ol>
     *     <li>先失效 current；CLOSED 重复调用直接返回。</li>
     *     <li>调用子类逆序释放资源，并在 finally 无条件发布 CLOSED。</li>
     * </ol>
     */
    @Override
    public final void close() {
        // 1、重复 close 不重复释放 child/cursor。
        if (state == PlanNodeState.CLOSED) {
            return;
        }
        current = null;
        try {
            // 2、closeNode 失败也不能让节点恢复为可拉取状态。
            closeNode();
        } finally {
            state = PlanNodeState.CLOSED;
        }
    }

    /**
     * 打开当前节点；失败时模板仍调用 {@link #closeNode()} 收敛部分资源。
     *
     * @param context 当前语句唯一运行上下文
     */
    protected abstract void openNode(ExecutionContext context);

    /**
     * 返回下一行或 EOF 的 Java null；null 只作为包内 iterator 协议，不进入 SQL 值域。
     *
     * @return 下一行视图，EOF 时为 Java {@code null}
     */
    protected abstract SqlRowView advanceNode();

    /** 幂等释放当前节点及其 child 资源。 */
    protected abstract void closeNode();
}
