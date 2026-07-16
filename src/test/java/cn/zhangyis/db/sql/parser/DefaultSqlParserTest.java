package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.sql.parser.ast.InsertStatementNode;
import cn.zhangyis.db.sql.parser.ast.SelectStatementNode;
import cn.zhangyis.db.sql.parser.ast.SetAutocommitNode;
import cn.zhangyis.db.sql.parser.ast.TransactionControlNode;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** v1 grammar、framing 与 source position 验收。 */
class DefaultSqlParserTest {
    private final DefaultSqlParser parser = new DefaultSqlParser(4096);

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

    @Test
    void rejectsUnsupportedShapesAndBrokenFraming() {
        String[] invalid = {
                "", "INSERT INTO t VALUES (1)", "INSERT INTO t(id) VALUES (1),(2)",
                "SELECT * FROM t", "SELECT * FROM t WHERE id > 1", "SELECT * FROM t WHERE id=1 OR x=2",
                "SELECT * FROM t WHERE id=1; SELECT 1", "INSERT INTO t(id) VALUES (X'ABC')",
                "INSERT INTO t(id) VALUES (B'012')", "SELECT * FROM `t WHERE id=1", "BEGIN garbage"
        };
        for (String sql : invalid) {
            assertThrows(SqlSyntaxException.class, () -> parser.parse(sql), sql);
        }
        assertThrows(SqlSyntaxException.class, () -> new DefaultSqlParser(4).parse("SELECT"));
    }

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
