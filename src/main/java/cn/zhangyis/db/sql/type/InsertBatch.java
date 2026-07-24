package cn.zhangyis.db.sql.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 一个 INSERT statement 的完整值源批次。所有行宽一致，行顺序就是自增分配和物理写入顺序。
 *
 * @param rows 非空、等宽且不含 Java {@code null} 的值源矩阵
 */
public record InsertBatch(List<List<InsertValueSource>> rows) {

    public InsertBatch {
        if (rows == null || rows.isEmpty()
                || rows.stream().anyMatch(row -> row == null || row.isEmpty()
                || row.stream().anyMatch(java.util.Objects::isNull))) {
            throw new DatabaseValidationException(
                    "INSERT batch requires non-empty rows and value sources");
        }
        int width = rows.getFirst().size();
        if (rows.stream().anyMatch(row -> row.size() != width)) {
            throw new DatabaseValidationException(
                    "INSERT batch rows must have the same width");
        }
        rows = rows.stream().map(List::copyOf).toList();
    }

    /**
     * 把 v1 单行 typed values 提升为常量批次。
     *
     * @param values 按 table ordinal 排列的完整单行
     * @return 仅包含 Constant 来源的一行批次
     */
    public static InsertBatch constants(List<SqlValue> values) {
        if (values == null) {
            throw new DatabaseValidationException(
                    "INSERT constant row must not be null");
        }
        return new InsertBatch(List.of(values.stream()
                .map(InsertValueSource.Constant::new)
                .map(InsertValueSource.class::cast)
                .toList()));
    }

    /**
     * @return 每行固定列数
     */
    public int width() {
        return rows.getFirst().size();
    }
}
