package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;
/**
 * VALUES INSERT 的不可变 AST。列列表为空表示按表定义顺序提供；rows 保存同一 statement 中的全部
 * 行构造器，Parser 不在此阶段补默认值或分配自增值。
 *
 * @param table 尚未绑定 DD identity 的目标限定名
 * @param columns 可选显式列列表；空集合表示表定义顺序
 * @param rows 非空行列表；每一行必须非空且与显式列宽一致
 */
public record InsertStatementNode(QualifiedNameNode table, List<IdentifierNode> columns,
                                  List<List<LiteralNode>> rows) implements StatementNode {
    public InsertStatementNode {
        int explicitWidth = columns == null ? -1 : columns.size();
        if (table == null || columns == null || rows == null || rows.isEmpty()
                || columns.stream().anyMatch(java.util.Objects::isNull)
                || rows.stream().anyMatch(row -> row == null || row.isEmpty()
                || row.stream().anyMatch(java.util.Objects::isNull))
                || !columns.isEmpty()
                && rows.stream().anyMatch(row -> row.size() != explicitWidth)) {
            throw new DatabaseValidationException("invalid INSERT AST shape");
        }
        columns = List.copyOf(columns);
        rows = rows.stream().map(List::copyOf).toList();
        int width = rows.getFirst().size();
        if (rows.stream().anyMatch(row -> row.size() != width)) {
            throw new DatabaseValidationException(
                    "INSERT row constructors must have the same width");
        }
    }

    /**
     * 返回首行以兼容只读 v1 测试与诊断代码；多行语义必须使用 {@link #rows()}。
     *
     * @return 第一行不可变字面量列表；构造器保证始终存在
     */
    public List<LiteralNode> values() {
        return rows.getFirst();
    }
}
