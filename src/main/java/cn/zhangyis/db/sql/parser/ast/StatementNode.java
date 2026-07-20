package cn.zhangyis.db.sql.parser.ast;

/** v1 SQL AST 根；纯语法对象，不依赖 DD/Session/storage。 */
public sealed interface StatementNode permits InsertStatementNode, SelectStatementNode,
        SetAutocommitNode, TransactionControlNode, UpdateStatementNode, DeleteStatementNode,
        CreateIndexStatementNode, DropIndexStatementNode { }
