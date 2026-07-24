package cn.zhangyis.db.sql.executor.row;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * SortNode 在 cursor 前进前复制出的独立行。它不持有 record/page/LOB 引用，可以安全写入 run 并跨拉取保存。
 */
public final class MaterializedSqlRowView implements SqlRowView {
    /** 与当前完整输入 schema 一一对应的不可变 SQL 值。 */
    private final List<SqlValue> values;
    /** compareLiteral 所需的 exact DD 类型。 */
    private final List<ResultColumn> columns;

    /**
     * @param values 已完全 hydrate 的行值
     * @param columns 与 values 等宽的 exact 输出列定义
     * @throws DatabaseValidationException 容器缺失、含 Java null 或宽度不一致时抛出
     */
    public MaterializedSqlRowView(
            List<SqlValue> values, List<ResultColumn> columns) {
        if (values == null || columns == null
                || values.size() != columns.size()
                || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "materialized sort row values/columns are invalid");
        }
        this.values = List.copyOf(values);
        this.columns = List.copyOf(columns);
    }

    @Override
    public int width() {
        return values.size();
    }

    @Override
    public SqlValue valueAt(int ordinal) {
        return values.get(requireOrdinal(ordinal));
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return values.get(requireOrdinal(ordinal))
                instanceof SqlValue.NullValue;
    }

    @Override
    public int compareLiteral(int ordinal, SqlValue literal) {
        int checked = requireOrdinal(ordinal);
        return SqlValueOrder.compare(
                values.get(checked), literal, columns.get(checked).type());
    }

    /**
     * 返回持久 run codec 可消费的不可变值列表。
     *
     * @return 与 columns 等宽的完全物化值
     */
    public List<SqlValue> values() {
        return values;
    }

    private int requireOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= values.size()) {
            throw new DatabaseValidationException(
                    "materialized sort row ordinal is outside schema");
        }
        return ordinal;
    }
}
