package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.sql.optimizer.DefaultSqlStatementCompiler;
import cn.zhangyis.db.sql.optimizer.HeuristicQueryOptimizer;
import cn.zhangyis.db.sql.optimizer.SqlStatementCompiler;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.BoundToLogicalConverter;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointSelect;
import cn.zhangyis.db.sql.parser.DefaultSqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 AST→Bound→Logical→Physical 编译边界与 statement metadata publish 的所有权。
 */
class SqlStatementCompilerTest {
    @TempDir
    Path directory;

    /**
     * 成功编译只返回物理计划而不发布 scope；Session 随后仍能完成唯一一次 publish。
     */
    @Test
    void leavesSuccessfulMetadataPublicationToSession() {
        DefaultSqlParser parser = new DefaultSqlParser();
        SqlStatementCompiler compiler = compiler();
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(301));
             StatementBindingScope statement =
                     transaction.beginStatement(Duration.ofSeconds(1))) {
            assertInstanceOf(PhysicalPointSelect.class, compiler.compile(
                    parser.parse(
                            "SELECT id FROM orders WHERE tenant=2 AND id=7"),
                    context(statement)));

            assertDoesNotThrow(statement::publish,
                    "Compiler 不得抢先发布 metadata；完整 physical plan 返回后由 Session 发布");
        }
    }

    /**
     * 优化阶段失败时 scope 仍由调用方拥有，关闭后不得遗留 staged MDL。
     */
    @Test
    void abortsStagedMetadataWhenOptimizationFails() {
        DefaultSqlParser parser = new DefaultSqlParser();
        SqlStatementCompiler compiler = compiler();
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(302))) {
            try (StatementBindingScope statement =
                         transaction.beginStatement(Duration.ofSeconds(1))) {
                assertThrows(SqlOptimizationException.class, () -> compiler.compile(
                        parser.parse(
                                "SELECT * FROM prefix_key WHERE code='x'"),
                        context(statement)));
            }
            assertTrue(fixture.locks.snapshot().granted().isEmpty(),
                    "Optimizer 失败后未 publish 的 statement scope 必须释放 staged MDL");
        }
    }

    private static SqlStatementCompiler compiler() {
        return new DefaultSqlStatementCompiler(
                new DefaultSqlBinder(new SqlTypeCoercion()),
                new BoundToLogicalConverter(), new HeuristicQueryOptimizer());
    }

    private static SqlBindingContext context(StatementBindingScope statement) {
        return new SqlBindingContext(
                Optional.of(ObjectName.of("app")), ZoneId.of("UTC"), statement);
    }
}
