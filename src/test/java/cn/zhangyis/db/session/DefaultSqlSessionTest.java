package cn.zhangyis.db.session;

import cn.zhangyis.db.sql.binder.DefaultSqlBinder;
import cn.zhangyis.db.sql.binder.SqlTypeCoercion;
import cn.zhangyis.db.sql.executor.*;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStatementRollbackException;
import cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionOutcomeException;
import cn.zhangyis.db.sql.parser.DefaultSqlParser;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.session.exception.SessionBusyException;
import cn.zhangyis.db.session.exception.SessionStateException;
import cn.zhangyis.db.session.exception.TransactionOutcomeUnknownException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Session parse→policy→bind→execute 与公平有界串行化的 TDD。 */
class DefaultSqlSessionTest {
    @TempDir Path directory;

    /** 控制命令发布正确 snapshot；普通 INSERT/SELECT 通过 binder/executor/gateway。 */
    @Test
    void executesCommandsAndDataStatements() {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            SessionTransactionPolicyTest.RecordingGateway gateway =
                    new SessionTransactionPolicyTest.RecordingGateway();
            DefaultSqlSession session = session(dictionary, gateway, options(Duration.ofSeconds(1)), () -> { });
            assertInstanceOf(UpdateResult.class,
                    session.execute("INSERT INTO orders (id) VALUES (1)"));
            assertEquals(SessionTransactionMode.NONE, session.snapshot().transactionMode());
            session.execute("SET autocommit=0");
            assertEquals(SessionTransactionMode.IMPLICIT, session.snapshot().transactionMode());
            assertInstanceOf(QueryResult.class, session.execute("SELECT * FROM orders WHERE id=1"));
            session.execute("BEGIN");
            assertEquals(SessionTransactionMode.EXPLICIT, session.snapshot().transactionMode());
            session.execute("ROLLBACK");
            assertEquals(SessionTransactionMode.IMPLICIT, session.snapshot().transactionMode());
            session.execute("SET autocommit=1");
            assertFalse(session.snapshot().transactionActive());
            session.close();
        }
    }

    /** 一个 execute 阻塞在 begin 时，第二个调用只等待 statement timeout，不能无界进入同一 Session。 */
    @Test
    void rejectsConcurrentExecuteAfterBoundedWait() throws Exception {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            BlockingGateway gateway = new BlockingGateway();
            DefaultSqlSession session = session(dictionary, gateway, options(Duration.ofMillis(80)), () -> { });
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var first = executor.submit(() -> session.execute("BEGIN"));
                assertTrue(gateway.entered.await(1, TimeUnit.SECONDS));
                assertThrows(SessionBusyException.class, () -> session.execute("BEGIN"));
                gateway.release.countDown();
                assertInstanceOf(CommandResult.class, first.get(1, TimeUnit.SECONDS));
            }
            session.close();
        }
    }

    /** execute 等待 Engine admission 时不得先占 Session 锁，否则 shutdown write gate 与 Session.close 会反向等待。 */
    @Test
    void engineAdmissionWaitDoesNotHoldSessionOperationLock() throws Exception {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            BlockingAdmission admission = new BlockingAdmission();
            DefaultSqlSession session = session(dictionary,
                    new SessionTransactionPolicyTest.RecordingGateway(), options(Duration.ofSeconds(1)),
                    admission, () -> { });
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var statement = executor.submit(() -> session.execute("BEGIN"));
                assertTrue(admission.entered.await(1, TimeUnit.SECONDS));
                assertEquals(SessionState.OPEN, session.snapshot().state(),
                        "waiting for engine admission must leave the Session operation lock available");
                admission.release.countDown();
                assertInstanceOf(CommandResult.class, statement.get(1, TimeUnit.SECONDS));
            }
            session.close();
        }
    }

    /** statement rollback 失败限制命令白名单；full rollback 后 Session 恢复普通状态。 */
    @Test
    void rollbackOnlyAllowsOnlyRollbackOrClose() {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            RollbackOnlyGateway gateway = new RollbackOnlyGateway();
            DefaultSqlSession session = session(dictionary, gateway, options(Duration.ofSeconds(1)), () -> { });
            session.execute("BEGIN");
            assertThrows(SqlStatementRollbackException.class,
                    () -> session.execute("INSERT INTO orders (id) VALUES (1)"));
            assertTrue(session.snapshot().rollbackOnly());
            assertThrows(SessionStateException.class,
                    () -> session.execute("SELECT * FROM orders WHERE id=1"));
            session.execute("ROLLBACK");
            assertFalse(session.snapshot().transactionActive());
            session.close();
        }
    }

    /** storage 已越过 commit 终态但响应失败时进入 FAILED，后续只允许 close。 */
    @Test
    void terminalCommitFailureMovesSessionToFailed() {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            TerminalCommitGateway gateway = new TerminalCommitGateway();
            AtomicBoolean deregistered = new AtomicBoolean();
            DefaultSqlSession session = session(dictionary, gateway, options(Duration.ofSeconds(1)),
                    () -> deregistered.set(true));
            session.execute("BEGIN");
            assertThrows(TransactionOutcomeUnknownException.class, () -> session.execute("COMMIT"));
            assertEquals(SessionState.FAILED, session.snapshot().state());
            assertThrows(SessionStateException.class, () -> session.execute("ROLLBACK"));
            session.close();
            assertTrue(deregistered.get());
        }
    }

    /** 物理写越过无 content-undo 的边界后必须 fail-closed，不能把 fatal 当作普通语句错误恢复 OPEN。 */
    @Test
    void fatalStorageFailureMovesSessionToFailed() {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            FatalInsertGateway gateway = new FatalInsertGateway();
            RecordingAdmission admission = new RecordingAdmission();
            DefaultSqlSession session = session(dictionary, gateway, options(Duration.ofSeconds(1)),
                    admission, () -> { });

            DatabaseFatalException fatal = assertThrows(DatabaseFatalException.class,
                    () -> session.execute("INSERT INTO orders (id) VALUES (1)"));
            assertEquals(SessionState.FAILED, session.snapshot().state());
            assertSame(fatal, admission.failure.get());
            assertThrows(SessionStateException.class,
                    () -> session.execute("SELECT * FROM orders WHERE id=1"));
            session.close();
        }
    }

    private static DefaultSqlSession session(SessionTestDictionary dictionary, SqlStorageGateway gateway,
                                             SessionOptions options, Runnable onClose) {
        return session(dictionary, gateway, options, SessionExecutionAdmission.unrestricted(), onClose);
    }

    private static DefaultSqlSession session(SessionTestDictionary dictionary, SqlStorageGateway gateway,
                                             SessionOptions options, SessionExecutionAdmission admission,
                                             Runnable onClose) {
        return new DefaultSqlSession(SessionId.of(1), options, dictionary.service,
                new DefaultSqlParser(), new DefaultSqlBinder(new SqlTypeCoercion()),
                new DefaultSqlExecutor(gateway), gateway, admission, onClose);
    }

    private static SessionOptions options(Duration statementTimeout) {
        Duration child = statementTimeout.compareTo(Duration.ofMillis(50)) > 0
                ? Duration.ofMillis(50) : statementTimeout;
        return new SessionOptions(Optional.of("app"), true, SqlIsolationLevel.REPEATABLE_READ,
                SqlDurabilityMode.BACKGROUND_FLUSH, ZoneId.of("UTC"), statementTimeout,
                child, child, child);
    }

    private static final class BlockingGateway extends SessionTransactionPolicyTest.RecordingGateway {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        @Override public SqlTransactionHandle begin(SqlTransactionRequest request) {
            entered.countDown();
            try {
                if (!release.await(1, TimeUnit.SECONDS)) throw new AssertionError("test release timeout");
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt(); throw new AssertionError(error);
            }
            return super.begin(request);
        }
    }

    private static final class RollbackOnlyGateway extends SessionTransactionPolicyTest.RecordingGateway {
        @Override public SqlWriteOutcome insert(SqlTransactionHandle transaction,
                                                cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert statement,
                                                SqlStatementDeadline deadline) {
            throw new SqlStatementRollbackException("statement rollback failed", true,
                    new cn.zhangyis.db.common.exception.DatabaseRuntimeException("injected"));
        }
    }

    private static final class TerminalCommitGateway extends SessionTransactionPolicyTest.RecordingGateway {
        @Override public SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request) {
            throw new SqlTransactionOutcomeException("terminal commit response failed", true, true,
                    new cn.zhangyis.db.common.exception.DatabaseRuntimeException("injected"));
        }
    }

    private static final class FatalInsertGateway extends SessionTransactionPolicyTest.RecordingGateway {
        @Override public SqlWriteOutcome insert(SqlTransactionHandle transaction,
                                                cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert statement,
                                                SqlStatementDeadline deadline) {
            throw new SqlStorageException("adapter wrapper",
                    new DatabaseFatalException("injected fail-stop boundary"));
        }
    }

    /** 记录 Session 向组合根发布的 fail-close 事件，同时不限制普通测试语句准入。 */
    private static final class RecordingAdmission implements SessionExecutionAdmission {
        private final AtomicReference<DatabaseFatalException> failure = new AtomicReference<>();

        @Override
        public Permit enter(SessionId sessionId, Duration timeout) {
            return () -> { };
        }

        @Override
        public void failClosed(DatabaseFatalException fatal) {
            failure.compareAndSet(null, fatal);
        }
    }

    /** 把 execute 暂停在组合根 gate 内，用于验证 gate 与 Session operation lock 的固定顺序。 */
    private static final class BlockingAdmission implements SessionExecutionAdmission {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Permit enter(SessionId sessionId, Duration timeout) {
            entered.countDown();
            try {
                if (!release.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    throw new SessionBusyException("test admission timed out");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new SessionBusyException("test admission interrupted", error);
            }
            return () -> { };
        }

        @Override
        public void failClosed(DatabaseFatalException failure) {
        }
    }
}
