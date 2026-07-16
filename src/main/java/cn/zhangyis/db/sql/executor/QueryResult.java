package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.HashSet;
import java.util.List;

/** 主键点查结果；v1 为零或一行，但模型保持普通不可变 row list。 */
public record QueryResult(List<ResultColumn> columns, List<SqlRow> rows,
                          TransactionStatus transactionStatus) implements SqlExecutionResult {
    public QueryResult {
        if (columns == null || rows == null || transactionStatus == null || columns.isEmpty()) {
            throw new DatabaseValidationException("query result fields/columns must not be null or empty");
        }
        columns = List.copyOf(columns);
        rows = List.copyOf(rows);
        HashSet<String> names = new HashSet<>();
        for (ResultColumn column : columns) {
            if (column == null || !names.add(column.name().toLowerCase(java.util.Locale.ROOT))) {
                throw new DatabaseValidationException("query result contains null/duplicate column");
            }
        }
        for (SqlRow row : rows) {
            if (row == null || row.values().size() != columns.size()) {
                throw new DatabaseValidationException("query result row width mismatch");
            }
        }
    }
}
