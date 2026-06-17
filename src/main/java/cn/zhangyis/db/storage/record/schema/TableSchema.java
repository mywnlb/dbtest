package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 表结构（不可变）。列按 ordinal 0..n-1 连续有序；记录编解码以此为权威列序与类型来源。
 *
 * <p>{@code clustered} 标志聚簇索引：聚簇 conventional 记录在用户字段区之后追加 15 字节隐藏区
 * （DB_TRX_ID + DB_ROLL_PTR）。它是 clustered 的**单一权威态**（{@code BTreeIndex.clustered()} 由此派生）。
 * 非聚簇 schema（含 node-pointer 派生 schema、二级索引）{@code clustered=false}，编码字节与隐藏列引入前逐位一致。
 *
 * @param schemaVersion schema 版本，编解码时校验一致。
 * @param columns       有序列定义（防御性不可变副本）。
 * @param clustered     是否聚簇索引（true 时记录携带隐藏区）。
 */
public record TableSchema(long schemaVersion, List<ColumnDef> columns, boolean clustered) {

    public TableSchema {
        if (columns == null || columns.isEmpty()) {
            throw new DatabaseValidationException("table schema must have at least one column");
        }
        columns = List.copyOf(columns);
        for (int i = 0; i < columns.size(); i++) {
            ColumnDef c = columns.get(i);
            if (c.ordinal() != i || c.id().value() != i) {
                throw new DatabaseValidationException(
                        "column ordinal/id must equal position " + i + ": " + c.name());
            }
        }
    }

    /**
     * 兼容副构造器：默认非聚簇（{@code clustered=false}）。保留既有 {@code (schemaVersion, columns)} 两参调用点
     * （record R1-R5、B1/B2/B3、node-pointer 派生 schema）源码与字节不破。
     */
    public TableSchema(long schemaVersion, List<ColumnDef> columns) {
        this(schemaVersion, columns, false);
    }

    /** 列数。 */
    public int columnCount() {
        return columns.size();
    }

    /** 按 ordinal 取列；越界抛校验异常。 */
    public ColumnDef column(int ordinal) {
        if (ordinal < 0 || ordinal >= columns.size()) {
            throw new DatabaseValidationException("column ordinal out of range: " + ordinal);
        }
        return columns.get(ordinal);
    }
}
