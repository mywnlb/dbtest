package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.sql.binder.StatementBindingScope;
import cn.zhangyis.db.sql.binder.TransactionMetadataScope;
import cn.zhangyis.db.sql.executor.TransactionStatus;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionOutcomeException;
import cn.zhangyis.db.session.exception.SessionStateException;

import java.time.Duration;

/**
 * Session transaction 状态表的单一实现。active transaction 同时拥有 opaque storage handle 与 metadata scope；
 * commit/rollback 先取得 storage 终态，随后才关闭 pin/MDL，失败前绝不提前释放 rollback 所需表定义。
 */
public final class SessionTransactionPolicy implements AutoCloseable {
    private final SessionOptions options;
    private final SqlStorageGateway gateway;
    private final DataDictionaryService dictionary;
    private final MdlOwnerId owner;
    private boolean autocommit;
    private ActiveTransaction active;
    private boolean rollbackOnly;
    private boolean closed;

    public SessionTransactionPolicy(SessionOptions options, SqlStorageGateway gateway,
                                    DataDictionaryService dictionary, MdlOwnerId owner) {
        if (options == null || gateway == null || dictionary == null || owner == null) {
            throw new DatabaseValidationException("transaction policy collaborators must not be null");
        }
        this.options = options;
        this.gateway = gateway;
        this.dictionary = dictionary;
        this.owner = owner;
        this.autocommit = options.autocommit();
        if (!autocommit) active = begin(false, SessionTransactionMode.IMPLICIT);
    }

    /** 普通语句准入：autocommit 创建临时 RO/RW 事务；autocommit=0 的缺失 implicit 状态会被自愈创建。 */
    public void prepareData(boolean readOnly) {
        ensureOpen();
        if (rollbackOnly) throw new SessionStateException("rollback-only transaction accepts only ROLLBACK/close");
        if (active == null) {
            active = begin(autocommit && readOnly,
                    autocommit ? SessionTransactionMode.AUTOCOMMIT_STATEMENT : SessionTransactionMode.IMPLICIT);
        }
    }

    /** 当前 active transaction 的 metadata statement staging scope。 */
    public StatementBindingScope beginBinding(Duration timeout) {
        requireActive();
        return active.metadata.beginStatement(timeout);
    }

    /** 当前 active opaque handle；只交给 SQL executor。 */
    public SqlTransactionHandle handle() {
        requireActive();
        return active.handle;
    }

    /** autocommit 普通语句成功后提交临时事务。 */
    public void completeAutocommit(Duration remaining) {
        requireMode(SessionTransactionMode.AUTOCOMMIT_STATEMENT);
        finishCommit(remaining);
    }

    /** autocommit 普通语句失败后完整回滚临时事务。 */
    public void failAutocommit() {
        requireMode(SessionTransactionMode.AUTOCOMMIT_STATEMENT);
        finishRollback();
    }

    /** BEGIN：已有 implicit/explicit 先隐式提交，再创建新的 explicit transaction。 */
    public void beginExplicit(Duration remaining) {
        ensureOpen();
        if (rollbackOnly) throw new SessionStateException("rollback-only transaction cannot BEGIN");
        if (active != null) finishCommit(remaining);
        active = begin(false, SessionTransactionMode.EXPLICIT);
    }

    /** SET autocommit 按设计先完成所需 storage 动作，再发布对用户可见变量。 */
    public void setAutocommit(boolean enabled, Duration remaining) {
        ensureOpen();
        if (enabled == autocommit) return;
        if (enabled) {
            if (rollbackOnly) throw new SessionStateException("rollback-only transaction cannot enable autocommit");
            if (active != null) finishCommit(remaining);
            autocommit = true;
            return;
        }
        // 变量先临时翻转；begin 失败恢复，调用方永远看不到无 implicit transaction 的 autocommit=false 发布态。
        autocommit = false;
        try {
            active = begin(false, SessionTransactionMode.IMPLICIT);
        } catch (RuntimeException beginFailure) {
            autocommit = true;
            throw beginFailure;
        }
    }

    /** COMMIT；autocommit=false 成功终结后立即创建新的 implicit transaction。NONE 为 no-op。 */
    public void commitAndContinue(Duration remaining) {
        ensureOpen();
        if (active == null) return;
        if (rollbackOnly) throw new SessionStateException("rollback-only transaction cannot COMMIT");
        finishCommit(remaining);
        if (!autocommit) active = begin(false, SessionTransactionMode.IMPLICIT);
    }

    /** ROLLBACK；autocommit=false 成功终结后立即创建新的 implicit transaction。NONE 为 no-op。 */
    public void rollbackAndContinue(Duration remaining) {
        ensureOpen();
        if (active == null) return;
        finishRollback();
        if (!autocommit) active = begin(false, SessionTransactionMode.IMPLICIT);
    }

    /** statement rollback 未确认时发布本地白名单状态；真实事务 rollback-only 由 gateway/storage 维护。 */
    public void markRollbackOnly() {
        requireActive();
        rollbackOnly = true;
    }

    public boolean autocommit() { return autocommit; }
    public boolean rollbackOnly() { return rollbackOnly; }
    public SessionTransactionMode mode() { return active == null ? SessionTransactionMode.NONE : active.mode; }
    public boolean transactionActive() { return active != null; }
    public TransactionStatus status() { return new TransactionStatus(autocommit, active != null, rollbackOnly); }

    /** close 对活动 transaction 做 full rollback；成功或已确认终态才关闭 metadata。 */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (active != null) finishRollbackAllowClosed();
    }

    private ActiveTransaction begin(boolean readOnly, SessionTransactionMode mode) {
        SqlTransactionHandle handle = gateway.begin(new SqlTransactionRequest(options.isolationLevel(), readOnly,
                mode == SessionTransactionMode.AUTOCOMMIT_STATEMENT));
        return new ActiveTransaction(handle, new TransactionMetadataScope(dictionary, owner), mode);
    }

    private void finishCommit(Duration remaining) {
        ActiveTransaction finishing = requireActive();
        try {
            gateway.commit(finishing.handle, new SqlCommitRequest(options.durabilityMode(),
                    minPositive(options.durabilityTimeout(), remaining)));
        } catch (SqlTransactionOutcomeException outcome) {
            if (outcome.terminal()) clearTerminal(finishing, outcome);
            throw outcome;
        }
        clearTerminal(finishing, null);
    }

    private void finishRollback() {
        ActiveTransaction finishing = requireActive();
        try {
            gateway.rollback(finishing.handle);
        } catch (SqlTransactionOutcomeException outcome) {
            if (outcome.terminal()) clearTerminal(finishing, outcome);
            throw outcome;
        }
        clearTerminal(finishing, null);
    }

    private void finishRollbackAllowClosed() {
        ActiveTransaction finishing = active;
        try {
            gateway.rollback(finishing.handle);
        } catch (SqlTransactionOutcomeException outcome) {
            if (outcome.terminal()) clearTerminal(finishing, outcome);
            throw outcome;
        }
        clearTerminal(finishing, null);
    }

    /** metadata close 发生在 storage terminal 之后；close 异常附加到既有 outcome，active 已不再可复用。 */
    private void clearTerminal(ActiveTransaction finishing, RuntimeException primary) {
        RuntimeException failure = primary;
        try { finishing.metadata.close(); }
        catch (RuntimeException metadataFailure) {
            if (failure == null) failure = new SessionStateException("terminal transaction metadata close failed",
                    metadataFailure); else failure.addSuppressed(metadataFailure);
        }
        active = null;
        rollbackOnly = false;
        if (primary == null && failure != null) throw failure;
    }

    private ActiveTransaction requireActive() {
        if (active == null) throw new SessionStateException("session has no active transaction");
        return active;
    }

    private void requireMode(SessionTransactionMode mode) {
        ActiveTransaction current = requireActive();
        if (current.mode != mode) throw new SessionStateException("transaction mode is " + current.mode
                + ", expected " + mode);
    }

    private void ensureOpen() {
        if (closed) throw new SessionStateException("transaction policy is closed");
    }

    private static Duration minPositive(Duration configured, Duration remaining) {
        if (remaining == null || remaining.isZero() || remaining.isNegative()) {
            throw new SessionStateException("statement deadline expired before transaction finalization");
        }
        return configured.compareTo(remaining) <= 0 ? configured : remaining;
    }

    /** active transaction 的双 owner 聚合；只有 policy 能替换/清除。 */
    private record ActiveTransaction(SqlTransactionHandle handle, TransactionMetadataScope metadata,
                                     SessionTransactionMode mode) { }
}
