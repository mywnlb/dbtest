package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.sql.binder.*;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
import cn.zhangyis.db.sql.executor.*;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStatementRollbackException;
import cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionOutcomeException;
import cn.zhangyis.db.sql.parser.DefaultSqlParser;
import cn.zhangyis.db.sql.parser.ast.*;
import cn.zhangyis.db.session.exception.SessionBusyException;
import cn.zhangyis.db.session.exception.SessionStateException;
import cn.zhangyis.db.session.exception.TransactionOutcomeUnknownException;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 进程内 Session 实现。公平显式锁串行化 execute/close；锁等待计入 statement deadline，任何外部 MDL/row/redo
 * 等待前只持 Session owner lock，不持页 latch/MTR。FAILED 状态只允许 close。
 */
public final class DefaultSqlSession implements SqlSession {
    private final SessionId id;
    private final SessionOptions options;
    private final DefaultSqlParser parser;
    private final DefaultSqlBinder binder;
    private final DefaultSqlExecutor executor;
    private final SessionTransactionPolicy transactions;
    private final Runnable onClose;
    /** Session 生命周期和 transaction policy 的唯一串行化 owner，公平避免 close/后续 execute 饥饿。 */
    private final ReentrantLock operationLock = new ReentrantLock(true);
    private final AtomicBoolean deregistered = new AtomicBoolean();
    private volatile SessionState state = SessionState.OPEN;

    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway, Runnable onClose) {
        if (id == null || options == null || dictionary == null || parser == null || binder == null
                || executor == null || gateway == null || onClose == null) {
            throw new DatabaseValidationException("session collaborators must not be null");
        }
        this.id = id;
        this.options = options;
        this.parser = parser;
        this.binder = binder;
        this.executor = executor;
        this.onClose = onClose;
        this.transactions = new SessionTransactionPolicy(options, gateway, dictionary, MdlOwnerId.of(id.value()));
    }

    @Override public SessionId id() { return id; }

    /** parse→transaction preparation→binding publish→execute→autocommit cleanup，共享一个绝对 statement deadline。 */
    @Override
    public SqlExecutionResult execute(String sql) {
        long deadline = deadline(options.statementTimeout());
        acquire(deadline, "execute");
        try {
            if (state != SessionState.OPEN) throw new SessionStateException("session cannot execute from state " + state);
            state = SessionState.EXECUTING;
            StatementNode statement = parser.parse(sql);
            if (transactions.rollbackOnly() && !isRollback(statement)) {
                throw new SessionStateException("rollback-only transaction accepts only ROLLBACK/close");
            }
            SqlExecutionResult result = switch (statement) {
                case SetAutocommitNode set -> executeSet(set, deadline);
                case TransactionControlNode control -> executeControl(control, deadline);
                case InsertStatementNode ignored -> executeData(statement, false, deadline);
                case SelectStatementNode ignored -> executeData(statement, true, deadline);
            };
            state = SessionState.OPEN;
            return result;
        } catch (SqlTransactionOutcomeException outcome) {
            state = SessionState.FAILED;
            throw new TransactionOutcomeUnknownException("session " + id.value()
                    + " transaction outcome is uncertain", outcome);
        } catch (RuntimeException failure) {
            if (state == SessionState.EXECUTING) state = SessionState.OPEN;
            throw failure;
        } finally {
            operationLock.unlock();
        }
    }

    private SqlExecutionResult executeData(StatementNode syntax, boolean readOnly, long deadline) {
        transactions.prepareData(readOnly);
        boolean autocommitStatement = transactions.mode() == SessionTransactionMode.AUTOCOMMIT_STATEMENT;
        try (StatementBindingScope scope = transactions.beginBinding(
                capped(options.metadataLockTimeout(), remaining(deadline)))) {
            Optional<ObjectName> schema = options.currentSchema().map(ObjectName::of);
            BoundStatement bound = binder.bind(syntax, new SqlBindingContext(schema, options.zoneId(), scope));
            SqlExecutionResult result = executor.execute(transactions.handle(), bound, transactions.status());
            if (autocommitStatement) {
                transactions.completeAutocommit(remaining(deadline));
                return withStatus(result, transactions.status());
            }
            return withStatus(result, transactions.status());
        } catch (RuntimeException statementFailure) {
            if (statementFailure instanceof SqlStatementRollbackException && !autocommitStatement) {
                transactions.markRollbackOnly();
            }
            if (autocommitStatement && transactions.mode() == SessionTransactionMode.AUTOCOMMIT_STATEMENT) {
                try { transactions.failAutocommit(); }
                catch (RuntimeException rollbackFailure) {
                    statementFailure.addSuppressed(rollbackFailure);
                    if (rollbackFailure instanceof SqlTransactionOutcomeException outcome) throw outcome;
                }
            }
            throw statementFailure;
        }
    }

    private CommandResult executeSet(SetAutocommitNode set, long deadline) {
        transactions.setAutocommit(set.enabled(), remaining(deadline));
        return new CommandResult(transactions.status());
    }

    private CommandResult executeControl(TransactionControlNode control, long deadline) {
        switch (control.kind()) {
            case BEGIN -> transactions.beginExplicit(remaining(deadline));
            case COMMIT -> transactions.commitAndContinue(remaining(deadline));
            case ROLLBACK -> transactions.rollbackAndContinue(remaining(deadline));
        }
        return new CommandResult(transactions.status());
    }

    /** 快照也使用有界锁，避免读取 transaction policy 的一半状态转换。 */
    @Override
    public SessionSnapshot snapshot() {
        long deadline = deadline(options.statementTimeout());
        acquire(deadline, "snapshot");
        try {
            return new SessionSnapshot(id, state, transactions.autocommit(), transactions.mode(),
                    transactions.transactionActive(), transactions.rollbackOnly(), options.currentSchema());
        } finally {
            operationLock.unlock();
        }
    }

    /** close 与 execute 公平竞争；full rollback 后关闭 metadata，deregister 始终幂等且 close 异常保留。 */
    @Override
    public void close() {
        if (state == SessionState.CLOSED) return;
        long deadline = deadline(options.statementTimeout());
        acquire(deadline, "close");
        RuntimeException failure = null;
        try {
            if (state == SessionState.CLOSED) return;
            state = SessionState.CLOSING;
            try { transactions.close(); }
            catch (RuntimeException closeFailure) { failure = closeFailure; }
            if (deregistered.compareAndSet(false, true)) {
                try { onClose.run(); }
                catch (RuntimeException callbackFailure) {
                    if (failure == null) failure = callbackFailure; else failure.addSuppressed(callbackFailure);
                }
            }
            state = SessionState.CLOSED;
        } finally {
            operationLock.unlock();
        }
        if (failure != null) throw failure;
    }

    private void acquire(long deadline, String operation) {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) throw new SessionBusyException("session " + id.value() + " " + operation + " deadline expired");
        try {
            if (!operationLock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
                throw new SessionBusyException("session " + id.value() + " is busy during " + operation);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SessionBusyException("session " + id.value() + " wait interrupted", error);
        }
    }

    private static boolean isRollback(StatementNode statement) {
        return statement instanceof TransactionControlNode control
                && control.kind() == TransactionControlNode.Kind.ROLLBACK;
    }

    private static SqlExecutionResult withStatus(SqlExecutionResult result, TransactionStatus status) {
        return switch (result) {
            case QueryResult query -> new QueryResult(query.columns(), query.rows(), status);
            case UpdateResult update -> new UpdateResult(update.affectedRows(), status);
            case CommandResult ignored -> new CommandResult(status);
        };
    }

    private static long deadline(Duration timeout) {
        long now = System.nanoTime();
        long result;
        try { result = Math.addExact(now, timeout.toNanos()); }
        catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
        return result;
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) throw new SessionBusyException("statement deadline expired");
        return Duration.ofNanos(nanos);
    }

    private static Duration capped(Duration configured, Duration remaining) {
        return configured.compareTo(remaining) <= 0 ? configured : remaining;
    }
}
