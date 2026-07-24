package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalLimit;

import java.util.List;

/**
 * 投影前最终行窗口的流式 LIMIT。count=0 不打开 child；其它请求只拉取 offset+count 所需前缀，
 * 上层 Projection 只会看到窗口内行。
 */
public final class LimitNode extends AbstractPlanNode {
    /** 已完成 Filter 与可选排序、仍保持完整 table row 宽度的 child。 */
    private final PlanNode input;
    /** 不可变 offset/count。 */
    private final PhysicalLimit limit;
    /** child 是否已被本节点打开，用于 count=0 的无副作用关闭。 */
    private boolean inputOpen;
    /** 尚未跳过的 offset 行数。 */
    private long remainingOffset;
    /** 尚可发布的 count 行数。 */
    private long remainingCount;

    /**
     * @param input 已完成 Filter 与可选排序的完整行输入
     * @param limit 最终 offset/count
     * @throws DatabaseValidationException 参数缺失时抛出
     */
    public LimitNode(PlanNode input, PhysicalLimit limit) {
        if (input == null || limit == null) {
            throw new DatabaseValidationException(
                    "limit node input/limit must not be null");
        }
        this.input = input;
        this.limit = limit;
    }

    /**
     * 初始化计数；count=0 不打开下游，从而不创建 cursor、ReadView 或行锁。
     *
     * @param context 当前语句运行上下文
     */
    @Override
    protected void openNode(ExecutionContext context) {
        remainingOffset = limit.offset();
        remainingCount = limit.count();
        if (remainingCount > 0) {
            input.open(context);
            inputOpen = true;
        }
    }

    /**
     * 首次拉取时消费 offset，随后最多发布 count 行；不会多拉一行探测。
     *
     * @return 当前限制窗口内下一行，窗口结束或 child EOF 时为 Java null
     */
    @Override
    protected SqlRowView advanceNode() {
        if (remainingCount == 0) {
            return null;
        }
        while (remainingOffset > 0) {
            if (!input.advance()) {
                remainingCount = 0;
                return null;
            }
            remainingOffset--;
        }
        if (!input.advance()) {
            remainingCount = 0;
            return null;
        }
        remainingCount--;
        return input.current();
    }

    /** 只关闭实际打开过的 child。 */
    @Override
    protected void closeNode() {
        if (inputOpen) {
            inputOpen = false;
            input.close();
        }
    }

    @Override
    public List<ResultColumn> columns() {
        return input.columns();
    }
}
