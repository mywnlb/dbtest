package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.row.ProjectedSqlRowView;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;

import java.util.List;

/**
 * 按用户列顺序映射 child row 的零拷贝 Project；真正 SqlRow 只在 Executor 结果边界物化。
 */
public final class ProjectionNode extends AbstractPlanNode {
    /** 已完成最终 truth 判断的 child。 */
    private final PlanNode input;
    /** 输出到 child ordinal 的不可变映射。 */
    private final List<Integer> ordinals;
    /** 与投影视图宽度一致的公开列元数据。 */
    private final List<ResultColumn> columns;

    /**
     * @param input Filter child
     * @param ordinals exact table column ordinal
     * @throws DatabaseValidationException 输入或 ordinal 无效时抛出
     */
    public ProjectionNode(PlanNode input, List<Integer> ordinals) {
        if (input == null || ordinals == null || ordinals.isEmpty()) {
            throw new DatabaseValidationException(
                    "projection node input/ordinals must not be null or empty");
        }
        List<ResultColumn> inputColumns = input.columns();
        for (Integer ordinal : ordinals) {
            if (ordinal == null || ordinal < 0
                    || ordinal >= inputColumns.size()) {
                throw new DatabaseValidationException(
                        "projection node ordinal is outside child schema");
            }
        }
        this.input = input;
        this.ordinals = List.copyOf(ordinals);
        this.columns = this.ordinals.stream()
                .map(inputColumns::get)
                .toList();
    }

    /**
     * 打开已完成最终 truth 判断的 child。
     *
     * @param context 当前语句运行上下文
     */
    @Override
    protected void openNode(ExecutionContext context) {
        input.open(context);
    }

    /**
     * 拉取一条匹配行并创建零拷贝 ordinal 视图；此阶段不主动读取未投影列。
     *
     * @return 投影视图，child EOF 时为 Java {@code null}
     */
    @Override
    protected SqlRowView advanceNode() {
        if (!input.advance()) {
            return null;
        }
        return new ProjectedSqlRowView(input.current(), ordinals);
    }

    /** 逆序关闭 Filter child。 */
    @Override
    protected void closeNode() {
        input.close();
    }

    @Override
    public List<ResultColumn> columns() {
        return columns;
    }
}
