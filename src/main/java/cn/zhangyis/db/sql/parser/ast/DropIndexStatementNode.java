package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 独立 {@code DROP INDEX} 与 {@code ALTER TABLE ... DROP INDEX} 归一后的纯语法 AST。
 * Parser 只保存用户给出的表名和索引名，不读取数据字典，也不判断目标是否为聚簇索引。
 *
 * @param table 目标限定表名；一段名称由 Binder 使用 Session 当前 schema 补全
 * @param indexName 待删除索引的语法标识符；是否存在由持 table MDL X 的 DD coordinator 复核
 */
public record DropIndexStatementNode(QualifiedNameNode table, IdentifierNode indexName)
        implements StatementNode {

    /**
     * 冻结 DROP INDEX 纯语法身份。
     *
     * @throws DatabaseValidationException 表名或索引名缺失时抛出，且不发布半初始化 AST
     */
    public DropIndexStatementNode {
        if (table == null || indexName == null) {
            throw new DatabaseValidationException("DROP INDEX AST fields must not be null");
        }
    }
}
