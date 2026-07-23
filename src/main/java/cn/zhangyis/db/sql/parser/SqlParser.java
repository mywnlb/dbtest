package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.ast.StatementNode;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

/**
 * SQL 文本到不可变语法 AST 的稳定边界。实现只负责词法/语法，不访问 DD、事务或存储。
 */
public interface SqlParser {

    /**
     * 解析并完整消费一条 SQL 语句。
     *
     * @param sql 非空 SQL 文本；可包含一个末尾分号
     * @return 不含名称绑定或物理访问路径的不可变 AST
     * @throws DatabaseValidationException SQL 引用为 Java {@code null} 时抛出；不会创建 AST
     * @throws SqlSyntaxException 文本超限、token 非法、语法不完整或存在尾随输入时抛出
     */
    StatementNode parse(String sql);
}
