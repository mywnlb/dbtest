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
import cn.zhangyis.db.sql.parser.ast.BetweenPredicateNode;
import cn.zhangyis.db.sql.parser.ast.BooleanExpressionNode;
import cn.zhangyis.db.sql.parser.ast.ConjunctionExpressionNode;
import cn.zhangyis.db.sql.parser.ast.DisjunctionExpressionNode;
import cn.zhangyis.db.sql.parser.ast.NegationExpressionNode;
import cn.zhangyis.db.sql.parser.ast.NullTestOperator;
import cn.zhangyis.db.sql.parser.ast.NullTestPredicateNode;
import cn.zhangyis.db.sql.parser.ast.PredicateNode;
import cn.zhangyis.db.sql.parser.ast.SavepointStatementNode;
import cn.zhangyis.db.sql.parser.ast.ComparisonOperator;
import cn.zhangyis.db.sql.parser.ast.ComparisonPredicateNode;
import cn.zhangyis.db.sql.parser.ast.XaStatementNode;
import cn.zhangyis.db.sql.parser.ast.AlterTableStatementNode;
import cn.zhangyis.db.sql.parser.ast.AlterTablespaceStatementNode;
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
        assertEquals(2, conjuncts(select.condition()).size());
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

    /** 命名保存点三种命令必须保留名称，并接受 MySQL 的 ROLLBACK TO 可选 SAVEPOINT 关键字。 */
    @Test
    void parsesNamedSavepointLifecycle() {
        SavepointStatementNode create = assertInstanceOf(SavepointStatementNode.class,
                parser.parse("SAVEPOINT BeforeBatch"));
        assertEquals(SavepointStatementNode.Kind.CREATE, create.kind());
        assertEquals("BeforeBatch", create.name().value());

        assertEquals(SavepointStatementNode.Kind.ROLLBACK_TO,
                assertInstanceOf(SavepointStatementNode.class,
                        parser.parse("ROLLBACK TO SAVEPOINT beforebatch")).kind());
        assertEquals("beforebatch", assertInstanceOf(SavepointStatementNode.class,
                parser.parse("ROLLBACK TO beforebatch")).name().value());
        assertEquals(SavepointStatementNode.Kind.RELEASE,
                assertInstanceOf(SavepointStatementNode.class,
                        parser.parse("RELEASE SAVEPOINT beforebatch;")).kind());
    }

    /**
     * XA grammar 必须保留字节 XID、signed format id 与命令专属选项，并严格拒绝非法尾部。
     */
    @Test
    void parsesCompleteXaGrammarAndOptions() {
        XaStatementNode start = assertInstanceOf(XaStatementNode.class,
                parser.parse("XA BEGIN X'0102', 'branch', -7 JOIN"));
        assertEquals(XaStatementNode.Kind.START, start.kind());
        assertEquals(XaStatementNode.StartMode.JOIN, start.startMode());
        assertArrayEquals(new byte[]{1, 2}, start.xid().orElseThrow().gtrid());
        assertEquals(-7, start.xid().orElseThrow().formatId());

        XaStatementNode end = assertInstanceOf(XaStatementNode.class,
                parser.parse("XA END 'global' SUSPEND FOR MIGRATE"));
        assertEquals(XaStatementNode.EndMode.FOR_MIGRATE, end.endMode());
        assertEquals(XaStatementNode.Kind.PREPARE,
                assertInstanceOf(XaStatementNode.class,
                        parser.parse("XA PREPARE 'global'")).kind());
        assertTrue(assertInstanceOf(XaStatementNode.class,
                parser.parse("XA COMMIT 'global' ONE PHASE")).onePhase());
        assertEquals(XaStatementNode.Kind.ROLLBACK,
                assertInstanceOf(XaStatementNode.class,
                        parser.parse("XA ROLLBACK 'global'")).kind());
        assertTrue(assertInstanceOf(XaStatementNode.class,
                parser.parse("XA RECOVER CONVERT XID")).convertXid());

        assertThrows(SqlSyntaxException.class,
                () -> parser.parse("XA PREPARE 'global' JOIN"));
        assertThrows(SqlSyntaxException.class,
                () -> parser.parse("XA COMMIT 'global', '', 2147483648"));
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
        assertEquals(2, conjuncts(update.condition()).size());

        DeleteStatementNode delete = assertInstanceOf(DeleteStatementNode.class,
                parser.parse("DELETE FROM orders WHERE id=1;"));
        assertInstanceOf(PredicateNode.class, delete.condition());
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

    /**
     * 通用 ALTER 必须保留多 action 顺序、列位置/default/type 及 rename/options；DISCARD/IMPORT
     * 使用独立 AST，不能与结构 action 混为普通列表。
     */
    @Test
    void parsesOrderedBlockingAlterAndTablespaceLifecycle() {
        AlterTableStatementNode alter = assertInstanceOf(
                AlterTableStatementNode.class,
                parser.parse("""
                        ALTER TABLE app.orders
                          DEFAULT CHARACTER SET 45 COLLATE 255,
                          ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'new' FIRST,
                          ADD INDEX idx_status (status DESC),
                          COMMENT='orders v2',
                          RENAME TO archive.orders_v2
                        """));

        assertEquals(5, alter.actions().size());
        assertInstanceOf(AlterTableStatementNode.DefaultCharset.class, alter.actions().get(0));
        AlterTableStatementNode.AddColumn add = assertInstanceOf(
                AlterTableStatementNode.AddColumn.class, alter.actions().get(1));
        assertEquals(AlterTableStatementNode.PositionKind.FIRST, add.position().kind());
        assertFalse(add.type().nullable());
        assertEquals(32, add.type().length());
        assertInstanceOf(AlterTableStatementNode.AddIndex.class, alter.actions().get(2));
        assertInstanceOf(AlterTableStatementNode.Comment.class, alter.actions().get(3));
        assertInstanceOf(AlterTableStatementNode.Rename.class, alter.actions().get(4));

        AlterTablespaceStatementNode discard = assertInstanceOf(
                AlterTablespaceStatementNode.class,
                parser.parse("ALTER TABLE orders DISCARD TABLESPACE"));
        assertEquals(AlterTablespaceStatementNode.Action.DISCARD, discard.action());
        AlterTablespaceStatementNode imported = assertInstanceOf(
                AlterTablespaceStatementNode.class,
                parser.parse("ALTER TABLE orders IMPORT TABLESPACE"));
        assertEquals(AlterTablespaceStatementNode.Action.IMPORT, imported.action());
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
     * 比较与 BETWEEN 必须保留开闭边界语义；BETWEEN 内部的 AND 由谓词自身消费，
     * 外层 AND 仍继续形成同一 conjunction，不能被错误截断。
     */
    @Test
    void parsesComparisonAndBetweenPredicatesAcrossReadAndWriteStatements() {
        SelectStatementNode select = assertInstanceOf(SelectStatementNode.class,
                parser.parse("""
                        SELECT id FROM orders
                        WHERE tenant = 7 AND created_at >= '2026-01-01'
                          AND created_at < '2027-01-01' AND score BETWEEN 60 AND 100
                        FOR SHARE
                        """));

        ComparisonPredicateNode lower = assertInstanceOf(
                ComparisonPredicateNode.class, conjuncts(select.condition()).get(1));
        ComparisonPredicateNode upper = assertInstanceOf(
                ComparisonPredicateNode.class, conjuncts(select.condition()).get(2));
        BetweenPredicateNode between = assertInstanceOf(
                BetweenPredicateNode.class, conjuncts(select.condition()).get(3));
        assertEquals(ComparisonOperator.GREATER_THAN_OR_EQUAL, lower.operator());
        assertEquals(ComparisonOperator.LESS_THAN, upper.operator());
        assertEquals("score", between.column().value());
        assertEquals(SelectLockingClause.FOR_SHARE, select.lockingClause());

        UpdateStatementNode update = assertInstanceOf(UpdateStatementNode.class,
                parser.parse("UPDATE orders SET status='old' WHERE id>10 AND id<=20"));
        assertEquals(ComparisonOperator.GREATER_THAN,
                assertInstanceOf(ComparisonPredicateNode.class,
                        conjuncts(update.condition()).getFirst()).operator());
        assertEquals(ComparisonOperator.LESS_THAN_OR_EQUAL,
                assertInstanceOf(ComparisonPredicateNode.class,
                        conjuncts(update.condition()).getLast()).operator());

        DeleteStatementNode delete = assertInstanceOf(DeleteStatementNode.class,
                parser.parse("DELETE FROM orders WHERE created_at BETWEEN '2020-01-01' AND '2020-12-31'"));
        assertInstanceOf(BetweenPredicateNode.class, delete.condition());
    }

    /**
     * 布尔表达式必须按 NOT、AND、OR 的优先级形成语法树；括号只改变树形，
     * 不作为无执行语义的独立节点泄漏给 Binder。
     */
    @Test
    void parsesBooleanPrecedenceParenthesesAndNullTests() {
        SelectStatementNode select = assertInstanceOf(SelectStatementNode.class,
                parser.parse("""
                        SELECT id FROM orders
                        WHERE id=1 OR NOT (status IS NULL AND tenant>=2)
                        """));

        DisjunctionExpressionNode disjunction = assertInstanceOf(
                DisjunctionExpressionNode.class, select.condition());
        assertInstanceOf(PredicateNode.class, disjunction.operands().getFirst());
        NegationExpressionNode negation = assertInstanceOf(
                NegationExpressionNode.class, disjunction.operands().getLast());
        ConjunctionExpressionNode conjunction = assertInstanceOf(
                ConjunctionExpressionNode.class, negation.operand());
        NullTestPredicateNode nullTest = assertInstanceOf(
                NullTestPredicateNode.class, conjunction.operands().getFirst());
        assertEquals(NullTestOperator.IS_NULL, nullTest.operator());
        assertEquals("status", nullTest.column().value());

        DeleteStatementNode delete = assertInstanceOf(DeleteStatementNode.class,
                parser.parse("DELETE FROM orders WHERE note IS NOT NULL"));
        assertEquals(NullTestOperator.IS_NOT_NULL,
                assertInstanceOf(NullTestPredicateNode.class,
                        delete.condition()).operator());
    }

    /**
     * 验证 {@code rejectsUnsupportedShapesAndBrokenFraming} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsUnsupportedShapesAndBrokenFraming() {
        String[] invalid = {
                "", "INSERT INTO t VALUES (1)", "INSERT INTO t(id) VALUES (1),(2)",
                "SELECT * FROM t", "SELECT * FROM t WHERE id <> 1",
                "SELECT * FROM t WHERE id=1; SELECT 1", "INSERT INTO t(id) VALUES (X'ABC')",
                "INSERT INTO t(id) VALUES (B'012')", "SELECT * FROM `t WHERE id=1", "BEGIN garbage",
                "SELECT * FROM t WHERE id=1 FOR", "SELECT * FROM t WHERE id=1 FOR DELETE",
                "SELECT * FROM t WHERE id=1 FOR SHARE FOR UPDATE"
                , "CREATE INDEX idx ON t (id(4))", "ALTER TABLE t ADD COLUMN",
                "CREATE FULLTEXT INDEX ft ON t (body)", "CREATE INDEX idx ON t ()",
                "DROP INDEX IF EXISTS idx ON t", "DROP INDEX idx t",
                "ALTER TABLE t DROP PRIMARY KEY", "ALTER TABLE t CONVERT TO CHARACTER SET 45",
                "SELECT * FROM t WHERE id <", "SELECT * FROM t WHERE id BETWEEN 1",
                "SELECT * FROM t WHERE id BETWEEN 1 OR 2", "SELECT * FROM t WHERE id ! 1",
                "SELECT * FROM t WHERE (id=1", "SELECT * FROM t WHERE id=1)",
                "SELECT * FROM t WHERE id IS", "SELECT * FROM t WHERE id IS TRUE",
                "SELECT * FROM t WHERE id NOT BETWEEN 1 AND 2"
                , "SAVEPOINT", "ROLLBACK TO", "ROLLBACK TO SAVEPOINT",
                "RELEASE beforebatch", "RELEASE SAVEPOINT"
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
        assertEquals("ID",
                assertInstanceOf(PredicateNode.class,
                        node.condition()).column().value());
    }

    /**
     * 把 Parser 产生的最外层 conjunction 展开为测试观察列表；单谓词视为单元素，
     * OR/NOT 不允许被误当成 AND 展开。
     *
     * @param condition statement AST 中唯一权威的 WHERE 条件
     * @return 保持用户书写顺序的最外层 AND operand
     */
    private static List<BooleanExpressionNode> conjuncts(
            BooleanExpressionNode condition) {
        if (condition instanceof ConjunctionExpressionNode conjunction) {
            return conjunction.operands();
        }
        return List.of(condition);
    }
}
