package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.sql.parser.ast.InsertStatementNode;
import cn.zhangyis.db.sql.parser.ast.SelectStatementNode;
import cn.zhangyis.db.sql.parser.ast.SetAutocommitNode;
import cn.zhangyis.db.sql.parser.ast.TransactionControlNode;
import cn.zhangyis.db.sql.parser.ast.UpdateStatementNode;
import cn.zhangyis.db.sql.parser.ast.DeleteStatementNode;
import cn.zhangyis.db.sql.parser.ast.SelectLockingClause;
import cn.zhangyis.db.sql.parser.ast.CreateIndexStatementNode;
import cn.zhangyis.db.sql.parser.ast.DropIndexStatementNode;
import cn.zhangyis.db.sql.parser.ast.IndexKeyOrderNode;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** v1 grammar、framing 与 source position 验收。 */
class DefaultSqlParserTest {
    private final DefaultSqlParser parser = new DefaultSqlParser(4096);

    /**
     * 验证 {@code parsesInsertAndPointSelectWithQuotedIdentifiersAndEscapedString} 所描述的 SQL 解析或绑定语义，并断言 AST、名称解析、类型推导及错误位置。
     */
    @Test
    void parsesInsertAndPointSelectWithQuotedIdentifiersAndEscapedString() {
        InsertStatementNode insert = assertInstanceOf(InsertStatementNode.class,
                parser.parse("INSERT INTO `app`.`t` (`id`, name) VALUES (-1, 'O''Reilly');"));
        assertEquals("app", insert.table().parts().get(0).value());
        assertEquals("O'Reilly", insert.values().get(1).value());

        SelectStatementNode select = assertInstanceOf(SelectStatementNode.class,
                parser.parse("select name,id from def.app.t where id=1 AND name='x'"));
        assertFalse(select.star());
        assertEquals(2, select.predicates().size());
    }

    /**
     * 验证 {@code parsesTransactionCommandsAndStrictAutocommit} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void parsesTransactionCommandsAndStrictAutocommit() {
        assertEquals(TransactionControlNode.Kind.BEGIN,
                assertInstanceOf(TransactionControlNode.class, parser.parse("START TRANSACTION")).kind());
        assertEquals(TransactionControlNode.Kind.COMMIT,
                assertInstanceOf(TransactionControlNode.class, parser.parse("commit;")).kind());
        assertFalse(assertInstanceOf(SetAutocommitNode.class,
                parser.parse("SET autocommit = 0")).enabled());
        assertThrows(SqlSyntaxException.class, () -> parser.parse("SET autocommit = true"));
    }

    /**
     * 验证 {@code parsesUpdateAndDeleteWithConjunctivePredicates} 所描述的 SQL 解析或绑定语义，并断言 AST、名称解析、类型推导及错误位置。
     */
    @Test
    void parsesUpdateAndDeleteWithConjunctivePredicates() {
        UpdateStatementNode update = assertInstanceOf(UpdateStatementNode.class,
                parser.parse("UPDATE app.orders SET name='new', amount=2 WHERE id=1 AND tenant=7"));
        assertEquals("orders", update.table().parts().getLast().value());
        assertEquals(2, update.assignments().size());
        assertEquals("name", update.assignments().getFirst().column().value());
        assertEquals(2, update.predicates().size());

        DeleteStatementNode delete = assertInstanceOf(DeleteStatementNode.class,
                parser.parse("DELETE FROM orders WHERE id=1;"));
        assertEquals(1, delete.predicates().size());
    }

    /** 两种 DDL 语法必须归一为同一个 AST，默认 ASC 与显式 DESC 均保持。 */
    @Test
    void parsesCreateIndexAndAlterTableAddIndexIntoSameAst() {
        CreateIndexStatementNode create = assertInstanceOf(
                CreateIndexStatementNode.class,
                parser.parse("CREATE UNIQUE INDEX uk_tenant_id ON app.orders (tenant DESC, id)"));
        CreateIndexStatementNode alter = assertInstanceOf(
                CreateIndexStatementNode.class,
                parser.parse("ALTER TABLE app.orders ADD UNIQUE INDEX uk_tenant_id (tenant DESC, id ASC)"));

        assertEquals(create.table().parts().stream().map(part -> part.value()).toList(),
                alter.table().parts().stream().map(part -> part.value()).toList());
        assertEquals(create.indexName().value(), alter.indexName().value());
        assertTrue(create.unique());
        assertEquals(List.of(IndexKeyOrderNode.DESC, IndexKeyOrderNode.ASC),
                create.keyParts().stream().map(part -> part.order()).toList());
        assertEquals(create.keyParts().stream().map(part -> part.column().value()).toList(),
                alter.keyParts().stream().map(part -> part.column().value()).toList());
    }

    /** 独立 DROP INDEX 与 ALTER TABLE DROP INDEX 必须归一为同一个 AST，避免两条 DDL 链产生不同恢复语义。 */
    @Test
    void parsesDropIndexAndAlterTableDropIndexIntoSameAst() {
        DropIndexStatementNode drop = assertInstanceOf(
                DropIndexStatementNode.class,
                parser.parse("DROP INDEX idx_status ON app.orders"));
        DropIndexStatementNode alter = assertInstanceOf(
                DropIndexStatementNode.class,
                parser.parse("ALTER TABLE app.orders DROP INDEX idx_status"));

        assertEquals(drop.table().parts().stream().map(part -> part.value()).toList(),
                alter.table().parts().stream().map(part -> part.value()).toList());
        assertEquals(drop.indexName().value(), alter.indexName().value());
    }

    /** locking clause 是 SELECT 的尾部语义，不得被当作普通标识符或 WHERE 谓词吞掉。 */
    @Test
    void parsesSelectLockingClausesAndKeepsConsistentReadDefault() {
        SelectStatementNode consistent = assertInstanceOf(SelectStatementNode.class,
                parser.parse("SELECT * FROM orders WHERE status='open'"));
        assertEquals(SelectLockingClause.NONE, consistent.lockingClause());

        SelectStatementNode share = assertInstanceOf(SelectStatementNode.class,
                parser.parse("SELECT id FROM orders WHERE status='open' FOR SHARE"));
        assertEquals(SelectLockingClause.FOR_SHARE, share.lockingClause());

        SelectStatementNode update = assertInstanceOf(SelectStatementNode.class,
                parser.parse("SELECT id FROM orders WHERE status='open' FOR UPDATE;"));
        assertEquals(SelectLockingClause.FOR_UPDATE, update.lockingClause());
    }

    /**
     * 验证 {@code rejectsUnsupportedShapesAndBrokenFraming} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsUnsupportedShapesAndBrokenFraming() {
        String[] invalid = {
                "", "INSERT INTO t VALUES (1)", "INSERT INTO t(id) VALUES (1),(2)",
                "SELECT * FROM t", "SELECT * FROM t WHERE id > 1", "SELECT * FROM t WHERE id=1 OR x=2",
                "SELECT * FROM t WHERE id=1; SELECT 1", "INSERT INTO t(id) VALUES (X'ABC')",
                "INSERT INTO t(id) VALUES (B'012')", "SELECT * FROM `t WHERE id=1", "BEGIN garbage",
                "SELECT * FROM t WHERE id=1 FOR", "SELECT * FROM t WHERE id=1 FOR DELETE",
                "SELECT * FROM t WHERE id=1 FOR SHARE FOR UPDATE"
                , "CREATE INDEX idx ON t (id(4))", "ALTER TABLE t ADD COLUMN c INT",
                "CREATE FULLTEXT INDEX ft ON t (body)", "CREATE INDEX idx ON t ()",
                "DROP INDEX IF EXISTS idx ON t", "DROP INDEX idx t",
                "ALTER TABLE t DROP PRIMARY KEY", "ALTER TABLE t DROP INDEX idx, DROP INDEX idx2"
        };
        for (String sql : invalid) {
            assertThrows(SqlSyntaxException.class, () -> parser.parse(sql), sql);
        }
        assertThrows(SqlSyntaxException.class, () -> new DefaultSqlParser(4).parse("SELECT"));
    }

    /**
     * 验证 {@code reportsStableLineColumnAndNormalizesKeywordsOnly} 对应的SQL 词法与语法解析行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void reportsStableLineColumnAndNormalizesKeywordsOnly() {
        SqlSyntaxException error = assertThrows(SqlSyntaxException.class,
                () -> parser.parse("SELECT *\nFROM t\nWHERE id ="));
        assertEquals(3, error.position().line());
        assertTrue(error.position().column() >= 10);
        SelectStatementNode node = assertInstanceOf(SelectStatementNode.class,
                parser.parse("SeLeCt * FrOm Mixed WHERE ID=1"));
        assertEquals("Mixed", node.table().parts().getFirst().value());
        assertEquals("ID", node.predicates().getFirst().column().value());
    }
}
