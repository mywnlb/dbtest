package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseFailureClassifier;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.sql.binder.*;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
import cn.zhangyis.db.sql.executor.*;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
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
    /** DatabaseEngine 用户语句 gate；permit 覆盖完整 execute 与失败 cleanup。 */
    private final SessionExecutionAdmission executionAdmission;
    /** Session 生命周期和 transaction policy 的唯一串行化 owner，公平避免 close/后续 execute 饥饿。 */
    private final ReentrantLock operationLock = new ReentrantLock(true);
    private final AtomicBoolean deregistered = new AtomicBoolean();
    private volatile SessionState state = SessionState.OPEN;

    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway, Runnable onClose) {
        this(id, options, dictionary, parser, binder, executor, gateway,
                SessionExecutionAdmission.unrestricted(), onClose);
    }

    /** DatabaseEngine 组合根构造入口；显式注入共享 statement admission gate。 */
    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway, SessionExecutionAdmission executionAdmission,
                             Runnable onClose) {
        if (id == null || options == null || dictionary == null || parser == null || binder == null
                || executor == null || gateway == null || executionAdmission == null || onClose == null) {
            throw new DatabaseValidationException("session collaborators must not be null");
        }
        this.id = id;
        this.options = options;
        this.parser = parser;
        this.binder = binder;
        this.executor = executor;
        this.onClose = onClose;
        this.executionAdmission = executionAdmission;
        this.transactions = new SessionTransactionPolicy(options, gateway, dictionary,
                MdlOwnerId.forSession(id.value()));
    }

    @Override public SessionId id() { return id; }

    /**
     * parse→transaction preparation→binding publish→execute→autocommit cleanup，共享一个绝对 statement deadline。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建唯一绝对 deadline，并先取得 Engine read permit；失败时尚未持有 Session 锁或任何事务资源。</li>
     *     <li>在剩余预算内取得 Session 串行锁，复核自身 OPEN 后进入 EXECUTING 并解析 SQL。</li>
     *     <li>按语句类型驱动 transaction policy、binder、executor 和 storage；所有子等待只消费同一剩余预算。</li>
     *     <li>按异常图分类 fatal/outcome-unknown/普通语句失败：fatal 同时发布 Engine fail-close，普通失败才恢复 OPEN。</li>
     *     <li>无论成功或异常都先释放 Session 锁、再释放 Engine permit，保持固定锁序且覆盖失败 cleanup 全链。</li>
     * </ol>
     */
    @Override
    public SqlExecutionResult execute(String sql) {
        // 1. admission 在 Session 锁外执行，保证 shutdown write gate 不会反等一个尚未取得 read permit 的 execute。
        SqlStatementDeadline deadline = SqlStatementDeadline.after(options.statementTimeout());
        SessionExecutionAdmission.Permit permit = executionAdmission.enter(
                id, deadline.remaining("engine statement admission"));
        boolean acquired = false;
        try {
            // 2. Session 内状态转换只有 operationLock owner 可见；非 OPEN 在 parser/storage 前拒绝。
            acquire(deadline, "execute");
            acquired = true;
            if (state != SessionState.OPEN) throw new SessionStateException("session cannot execute from state " + state);
            state = SessionState.EXECUTING;
            StatementNode statement = parser.parse(sql);
            if (transactions.rollbackOnly() && !isRollback(statement)) {
                throw new SessionStateException("rollback-only transaction accepts only ROLLBACK/close");
            }
            // 3. transaction、MDL、row lock、LOB hydrate 与 durability 等待都从 deadline 读取剩余预算。
            SqlExecutionResult result = switch (statement) {
                case SetAutocommitNode set -> executeSet(set, deadline);
                case TransactionControlNode control -> executeControl(control, deadline);
                case InsertStatementNode ignored -> executeData(statement, false, deadline);
                case SelectStatementNode ignored -> executeData(statement, true, deadline);
            };
            state = SessionState.OPEN;
            return result;
        } catch (RuntimeException failure) {
            // 4. cause/suppressed 中的 fatal 也不能被 facade 包装降级；只有普通失败允许 Session 重新 OPEN。
            DatabaseFatalException fatal = DatabaseFailureClassifier.preserveFatal(
                    "session " + id.value() + " observed a fail-stop storage error", failure);
            if (fatal != null) {
                state = SessionState.FAILED;
                try {
                    executionAdmission.failClosed(fatal);
                } catch (RuntimeException publicationFailure) {
                    fatal.addSuppressed(publicationFailure);
                }
                throw fatal;
            }
            if (failure instanceof SqlTransactionOutcomeException outcome) {
                state = SessionState.FAILED;
                throw new TransactionOutcomeUnknownException("session " + id.value()
                        + " transaction outcome is uncertain", outcome);
            }
            if (state == SessionState.EXECUTING) state = SessionState.OPEN;
            throw failure;
        } finally {
            // 5. permit 覆盖 Session 锁内全部 cleanup；释放顺序与获取顺序相反。
            if (acquired) operationLock.unlock();
            permit.close();
        }
    }

    /**
     * 执行一条已识别为数据访问的语句。
     * <ol>
     *     <li>按读写意图准备/复用事务，并记录它是否为本语句专属 autocommit transaction。</li>
     *     <li>以 deadline 限制 MDL 获取，完成 exact-version DD binding 后把 bound plan 交给 executor/storage。</li>
     *     <li>成功的 autocommit 语句先完成 storage commit，再让 binding scope 释放 transaction metadata。</li>
     *     <li>失败时显式事务只标记 rollback-only；autocommit 尝试 full rollback，二次失败作为 suppressed 保留。</li>
     * </ol>
     */
    private SqlExecutionResult executeData(StatementNode syntax, boolean readOnly, SqlStatementDeadline deadline) {
        // 1. prepareData 只建立事务上下文；实际写 id 仍由 storage 首写延迟分配。
        transactions.prepareData(readOnly);
        boolean autocommitStatement = transactions.mode() == SessionTransactionMode.AUTOCOMMIT_STATEMENT;
        // 2. metadata timeout 不得越过 statement 剩余预算，scope 只负责本次 binding 发布边界。
        try (StatementBindingScope scope = transactions.beginBinding(
                deadline.cap(options.metadataLockTimeout(), "metadata binding"))) {
            Optional<ObjectName> schema = options.currentSchema().map(ObjectName::of);
            BoundStatement bound = binder.bind(syntax, new SqlBindingContext(schema, options.zoneId(), scope));
            SqlExecutionResult result = executor.execute(transactions.handle(), bound, transactions.status(), deadline);
            // 3. storage 终态成功后 completeAutocommit 才清 transaction-duration MDL/pin。
            if (autocommitStatement) {
                transactions.completeAutocommit(deadline.remaining("autocommit finalization"));
                return withStatus(result, transactions.status());
            }
            return withStatus(result, transactions.status());
        } catch (RuntimeException statementFailure) {
            // 4. rollback 失败不能覆盖原始语句异常；外层异常分类仍会沿 suppressed 图识别 fatal/outcome unknown。
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

    private CommandResult executeSet(SetAutocommitNode set, SqlStatementDeadline deadline) {
        transactions.setAutocommit(set.enabled(), deadline.remaining("SET autocommit finalization"));
        return new CommandResult(transactions.status());
    }

    private CommandResult executeControl(TransactionControlNode control, SqlStatementDeadline deadline) {
        switch (control.kind()) {
            case BEGIN -> transactions.beginExplicit(deadline.remaining("BEGIN transition"));
            case COMMIT -> transactions.commitAndContinue(deadline.remaining("COMMIT transition"));
            case ROLLBACK -> transactions.rollbackAndContinue(deadline.remaining("ROLLBACK transition"));
        }
        return new CommandResult(transactions.status());
    }

    /** 快照也使用有界锁，避免读取 transaction policy 的一半状态转换。 */
    @Override
    public SessionSnapshot snapshot() {
        SqlStatementDeadline deadline = SqlStatementDeadline.after(options.statementTimeout());
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
        SqlStatementDeadline deadline = SqlStatementDeadline.after(options.statementTimeout());
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

    private void acquire(SqlStatementDeadline deadline, String operation) {
        long nanos;
        try {
            nanos = deadline.remaining("session " + operation + " lock").toNanos();
        } catch (cn.zhangyis.db.sql.executor.storage.exception.SqlStatementTimeoutException expired) {
            throw new SessionBusyException("session " + id.value() + " " + operation + " deadline expired", expired);
        }
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

}
