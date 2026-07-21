package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseFailureClassifier;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
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
import cn.zhangyis.db.session.xa.SessionXaCoordinator;

import java.time.Duration;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 进程内 Session 实现。公平显式锁串行化 execute/close；锁等待计入 statement deadline，任何外部 MDL/row/redo
 * 等待前只持 Session owner lock，不持页 latch/MTR。FAILED 状态只允许 close。
 */
public final class DefaultSqlSession implements SqlSession {
    /**
     * 构造时冻结的 {@code id} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final SessionId id;
    /**
     * 构造时冻结的 {@code options} 配置快照；已完成范围和组合校验，运行期策略读取它但不得就地修改。
     */
    private final SessionOptions options;
    /**
     * 本对象持有的 {@code parser} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DefaultSqlParser parser;
    /**
     * 本对象持有的 {@code binder} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DefaultSqlBinder binder;
    /**
     * 本对象持有的 {@code executor} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DefaultSqlExecutor executor;
    /**
     * 本对象持有的 {@code transactions} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SessionTransactionPolicy transactions;
    /** DDL 使用独立 MDL owner 的 DD coordinator port，不能复用普通 transaction gateway。 */
    private final cn.zhangyis.db.sql.executor.storage.SqlDdlGateway ddlGateway;
    /**
     * 构造时注入的 {@code onClose} 生命周期回调；只允许在类级契约声明的阶段调用，失败必须沿原异常路径传播。
     */
    private final Runnable onClose;
    /** DatabaseEngine 用户语句 gate；permit 覆盖完整 execute 与失败 cleanup。 */
    private final SessionExecutionAdmission executionAdmission;
    /** Session 生命周期和 transaction policy 的唯一串行化 owner，公平避免 close/后续 execute 饥饿。 */
    private final ReentrantLock operationLock = new ReentrantLock(true);
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private final AtomicBoolean deregistered = new AtomicBoolean();
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private volatile SessionState state = SessionState.OPEN;

    /**
     * 创建 {@code DefaultSqlSession}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param id 参与 {@code 构造} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param options 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param dictionary 由组合根提供的 {@code DataDictionaryService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param parser 由组合根提供的 {@code DefaultSqlParser} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param binder 由组合根提供的 {@code DefaultSqlBinder} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param executor 由组合根提供的 {@code DefaultSqlExecutor} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param gateway 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param onClose 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     */
    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway, Runnable onClose) {
        this(id, options, dictionary, parser, binder, executor, gateway,
                cn.zhangyis.db.sql.executor.storage.SqlDdlGateway.UNSUPPORTED,
                SessionXaCoordinator.UNSUPPORTED,
                SessionExecutionAdmission.unrestricted(), onClose);
    }

    /** DatabaseEngine 组合根构造入口；显式注入共享 statement admission gate。
     *
     * @param id 参与 {@code 构造} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param options 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param dictionary 由组合根提供的 {@code DataDictionaryService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param parser 由组合根提供的 {@code DefaultSqlParser} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param binder 由组合根提供的 {@code DefaultSqlBinder} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param executor 由组合根提供的 {@code DefaultSqlExecutor} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param gateway 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param executionAdmission SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param onClose 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     */
    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway, SessionExecutionAdmission executionAdmission,
                             Runnable onClose) {
        this(id, options, dictionary, parser, binder, executor, gateway,
                cn.zhangyis.db.sql.executor.storage.SqlDdlGateway.UNSUPPORTED,
                SessionXaCoordinator.UNSUPPORTED,
                executionAdmission, onClose);
    }

    /** DatabaseEngine 完整组合根构造入口；DDL port 与 DML transaction port 显式分离。
     *
     * @param id 参与 {@code 构造} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param options 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param dictionary 由组合根提供的 {@code DataDictionaryService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param parser 由组合根提供的 {@code DefaultSqlParser} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param binder 由组合根提供的 {@code DefaultSqlBinder} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param executor 由组合根提供的 {@code DefaultSqlExecutor} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param gateway 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param ddlGateway 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param executionAdmission SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param onClose 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway,
                             cn.zhangyis.db.sql.executor.storage.SqlDdlGateway ddlGateway,
                             SessionExecutionAdmission executionAdmission,
                             Runnable onClose) {
        this(id, options, dictionary, parser, binder, executor, gateway, ddlGateway,
                SessionXaCoordinator.UNSUPPORTED, executionAdmission, onClose);
    }

    /**
     * DatabaseEngine 完整组合根构造入口；显式注入实例级 XA coordinator。
     *
     * @param id Session 稳定身份
     * @param options Session SQL 与 timeout 语义
     * @param dictionary transaction metadata owner
     * @param parser SQL parser
     * @param binder SQL binder
     * @param executor SQL executor
     * @param gateway opaque storage transaction port
     * @param ddlGateway DDL port
     * @param xaCoordinator 实例共享 XID registry/coordinator
     * @param executionAdmission DatabaseEngine statement gate
     * @param onClose Session registry 注销回调
     */
    public DefaultSqlSession(SessionId id, SessionOptions options, DataDictionaryService dictionary,
                             DefaultSqlParser parser, DefaultSqlBinder binder, DefaultSqlExecutor executor,
                             SqlStorageGateway gateway,
                             cn.zhangyis.db.sql.executor.storage.SqlDdlGateway ddlGateway,
                             SessionXaCoordinator xaCoordinator,
                             SessionExecutionAdmission executionAdmission,
                             Runnable onClose) {
        if (id == null || options == null || dictionary == null || parser == null || binder == null
                || executor == null || gateway == null || ddlGateway == null
                || xaCoordinator == null || executionAdmission == null || onClose == null) {
            throw new DatabaseValidationException("session collaborators must not be null");
        }
        this.id = id;
        this.options = options;
        this.parser = parser;
        this.binder = binder;
        this.executor = executor;
        this.ddlGateway = ddlGateway;
        this.onClose = onClose;
        this.executionAdmission = executionAdmission;
        this.transactions = new SessionTransactionPolicy(options, gateway, dictionary,
                MdlOwnerId.forSession(id.value()), xaCoordinator);
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
     *
     * @param sql 传给 {@code execute} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code execute} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws TransactionOutcomeUnknownException SQL 绑定、会话准入或事务结果无法按当前状态完成时抛出；调用方应报告错误并按事务边界回滚或关闭
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
            if (statement instanceof SelectStatementNode select
                    && select.lockingClause() == SelectLockingClause.NONE
                    && transactions.requiresSerializableLockingRead()) {
                statement = new SelectStatementNode(select.star(), select.projections(), select.table(),
                        select.predicates(), SelectLockingClause.FOR_SHARE);
            }
            if (transactions.rollbackOnly() && !isRollback(statement)) {
                throw new SessionStateException("rollback-only transaction accepts only ROLLBACK/close");
            }
            // 3. transaction、MDL、row lock、LOB hydrate 与 durability 等待都从 deadline 读取剩余预算。
            SqlExecutionResult result = switch (statement) {
                case SetAutocommitNode set -> executeSet(set, deadline);
                case TransactionControlNode control -> executeControl(control, deadline);
                case SavepointStatementNode savepoint -> executeSavepoint(savepoint, deadline);
                case XaStatementNode xa -> executeXa(xa, deadline);
                case InsertStatementNode ignored -> executeData(statement, false, deadline);
                case SelectStatementNode select -> executeData(
                        statement, select.lockingClause() == SelectLockingClause.NONE, deadline);
                case UpdateStatementNode ignored -> executeData(statement, false, deadline);
                case DeleteStatementNode ignored -> executeData(statement, false, deadline);
                case CreateIndexStatementNode createIndex -> executeCreateIndex(createIndex, deadline);
                case DropIndexStatementNode dropIndex -> executeDropIndex(dropIndex, deadline);
                case AlterTablespaceStatementNode alterTablespace ->
                        executeAlterTablespace(alterTablespace, deadline);
                case AlterTableStatementNode alterTable ->
                        executeAlterTable(alterTable, deadline);
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
     *
     * @param syntax SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param readOnly 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code executeData} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
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

    /**
     * 绑定并执行 CREATE INDEX / ALTER ADD INDEX。语法绑定先于 implicit commit，避免纯名称错误意外提交事务；
     * DD coordinator 随后使用独立 owner 获取 table X。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param syntax SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code executeCreateIndex} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private CommandResult executeCreateIndex(CreateIndexStatementNode syntax,
                                             SqlStatementDeadline deadline) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        Optional<ObjectName> schema = options.currentSchema().map(ObjectName::of);
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        var bound = binder.bindDdl(syntax, schema);
        transactions.prepareDdl(deadline.remaining("DDL implicit commit"));
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        RuntimeException failure = null;
        try {
            ddlGateway.createSecondaryIndex(
                    bound, deadline.remaining("CREATE INDEX coordinator"));
        } catch (RuntimeException ddlFailure) {
            failure = ddlFailure;
            throw ddlFailure;
        } finally {
            try {
                transactions.resumeAfterDdl();
            } catch (RuntimeException resumeFailure) {
                if (failure == null) {
                    throw resumeFailure;
                }
                failure.addSuppressed(resumeFailure);
            }
        }
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return new CommandResult(transactions.status());
    }

    /**
     * 绑定并执行 DROP INDEX / ALTER DROP INDEX；名称绑定先于 implicit commit，目标存在性由 table X 下的
     * DD coordinator 重验。DDL 成功或失败后都恢复 Session transaction policy，不能把独立 DDL owner 泄漏给用户事务。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 Session 快照读取 current schema，把纯语法名称绑定为不持 DD lease 的命令。</li>
     *     <li>执行 DDL implicit commit，释放用户事务的 row lock、MDL 与 metadata pin。</li>
     *     <li>用剩余 deadline 调用独立 DDL gateway；其内部持 table X 并完成 DD/物理状态机。</li>
     *     <li>无论成功或失败都恢复 DDL 后事务模式；恢复失败作为原异常 suppressed，成功返回 CommandResult。</li>
     * </ol>
     *
     * @param syntax Parser 已归一的两种 DROP INDEX AST
     * @param deadline 本条 statement 的唯一绝对期限；所有下游等待只能消费其剩余值
     * @return 不携带 affected rows、但带最新 Session transaction status 的命令结果
     */
    private CommandResult executeDropIndex(DropIndexStatementNode syntax,
                                           SqlStatementDeadline deadline) {
        // 1、Binder 不打开 table，因此纯限定名错误早于 implicit commit。
        Optional<ObjectName> schema = options.currentSchema().map(ObjectName::of);
        var bound = binder.bindDdl(syntax, schema);
        // 2、DDL 不能复用用户事务或它持有的 transaction-duration MDL。
        transactions.prepareDdl(deadline.remaining("DDL implicit commit"));
        RuntimeException failure = null;
        try {
            // 3、gateway 创建独立 owner，DD coordinator 再取得 schema IX/table X。
            ddlGateway.dropSecondaryIndex(bound, deadline.remaining("DROP INDEX coordinator"));
        } catch (RuntimeException ddlFailure) {
            failure = ddlFailure;
            throw ddlFailure;
        } finally {
            // 4、保持 CREATE/DROP 共用的 Session 终态与异常图规则。
            try {
                transactions.resumeAfterDdl();
            } catch (RuntimeException resumeFailure) {
                if (failure == null) {
                    throw resumeFailure;
                }
                failure.addSuppressed(resumeFailure);
            }
        }
        return new CommandResult(transactions.status());
    }

    /**
     * 执行 DISCARD/IMPORT TABLESPACE，并与其它阻塞 DDL 共用“先绑定、再 implicit commit、最后独立
     * MDL owner”的 Session 边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>使用 Session 当前 schema 补全表名；纯名称错误发生在事务提交之前。</li>
     *     <li>完成 DDL implicit commit，确保用户 row lock、transaction MDL 与 metadata pin 已释放。</li>
     *     <li>把无路径的 bound 意图交给 DDL gateway；DD 在 table X 下选择受控 transfer 文件并执行状态机。</li>
     *     <li>无论 DDL 成败均恢复 Session policy；恢复失败附加到原异常，成功返回最新事务状态。</li>
     * </ol>
     *
     * @param syntax parser 产生的 DISCARD/IMPORT TABLESPACE AST
     * @param deadline 本条 SQL 的绝对 deadline
     * @return 不含 affected rows、携带 DDL 后 Session 状态的命令结果
     */
    private CommandResult executeAlterTablespace(AlterTablespaceStatementNode syntax,
                                                 SqlStatementDeadline deadline) {
        // 1、Binder 只补全逻辑名，不提前读取可能过期的 table state/binding。
        Optional<ObjectName> schema = options.currentSchema().map(ObjectName::of);
        var bound = binder.bindDdl(syntax, schema);
        // 2、先结束用户事务，避免独立 DDL owner 等待本会话自己的 transaction-duration 资源。
        transactions.prepareDdl(deadline.remaining("DDL implicit commit"));
        RuntimeException failure = null;
        try {
            // 3、下游在 table X 内派生固定路径、校验 page0 identity 并推进 DDL log。
            ddlGateway.alterTablespace(
                    bound, deadline.remaining("ALTER TABLESPACE coordinator"));
        } catch (RuntimeException ddlFailure) {
            failure = ddlFailure;
            throw ddlFailure;
        } finally {
            // 4、恢复 Session transaction policy，保留完整异常图。
            try {
                transactions.resumeAfterDdl();
            } catch (RuntimeException resumeFailure) {
                if (failure == null) {
                    throw resumeFailure;
                }
                failure.addSuppressed(resumeFailure);
            }
        }
        return new CommandResult(transactions.status());
    }

    /**
     * 执行通用阻塞式 ALTER。Binder 在 implicit commit 前完成类型/default 等纯 SQL 校验，DD 则在
     * table MDL X 下按 action 顺序构造并一次发布 staged definition。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 current schema/zone 并绑定无资源命令，纯输入错误不提交现有事务。</li>
     *     <li>执行 DDL implicit commit，释放用户事务持有的 row lock、MDL 和 metadata pin。</li>
     *     <li>一次调用独立 DDL gateway；多 action 不得拆分成多个可观察版本。</li>
     *     <li>无论成功失败均恢复 Session policy，成功返回 DDL 后事务状态。</li>
     * </ol>
     *
     * @param syntax 通用 ALTER AST
     * @param deadline 本条语句唯一绝对期限
     * @return DDL 后 Session 命令状态
     */
    private CommandResult executeAlterTable(AlterTableStatementNode syntax,
                                            SqlStatementDeadline deadline) {
        // 1、纯绑定阶段不打开 DD lease，table-dependent action 在 coordinator 内重验。
        Optional<ObjectName> schema = options.currentSchema().map(ObjectName::of);
        var bound = binder.bindDdl(syntax, schema, options.zoneId());
        // 2、独立 DDL owner 不得与用户事务资源形成自等待。
        transactions.prepareDdl(deadline.remaining("DDL implicit commit"));
        RuntimeException failure = null;
        try {
            // 3、整个 action list 只进入一次 coordinator。
            ddlGateway.alterTable(bound, deadline.remaining("ALTER TABLE coordinator"));
        } catch (RuntimeException ddlFailure) {
            failure = ddlFailure;
            throw ddlFailure;
        } finally {
            // 4、保持所有 DDL 共用的 Session 恢复与 suppressed 规则。
            try {
                transactions.resumeAfterDdl();
            } catch (RuntimeException resumeFailure) {
                if (failure == null) {
                    throw resumeFailure;
                }
                failure.addSuppressed(resumeFailure);
            }
        }
        return new CommandResult(transactions.status());
    }

    private CommandResult executeSet(SetAutocommitNode set, SqlStatementDeadline deadline) {
        transactions.setAutocommit(set.enabled(), deadline.remaining("SET autocommit finalization"));
        return new CommandResult(transactions.status());
    }

    /**
     * 执行已校验的领域命令并协调下游接口；成功返回可观察结果，失败保留原始异常与事务边界。
     *
     * @param control 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code executeControl} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private CommandResult executeControl(TransactionControlNode control, SqlStatementDeadline deadline) {
        switch (control.kind()) {
            case BEGIN -> transactions.beginExplicit(deadline.remaining("BEGIN transition"));
            case COMMIT -> transactions.commitAndContinue(deadline.remaining("COMMIT transition"));
            case ROLLBACK -> transactions.rollbackAndContinue(deadline.remaining("ROLLBACK transition"));
        }
        return new CommandResult(transactions.status());
    }

    /**
     * 执行命名保存点命令；Session 只传规范 SQL 名称和不透明 transaction 能力，不接触 undo/lock 对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>Parser 已保证名称和命令种类完整；Session operation lock 继续串行化同一事务的名称映射。</li>
     *     <li>把同一 statement deadline 传给 transaction policy，约束 gateway handle 获取。</li>
     *     <li>policy 按 CREATE/ROLLBACK_TO/RELEASE 更新 opaque 保存点映射，失败不发布半完成名称状态。</li>
     *     <li>返回当前事务状态；命令不隐式 commit，也不创建 autocommit 临时事务。</li>
     * </ol>
     *
     * @param savepoint Parser 产生的命名保存点 AST
     * @param deadline 当前 execute 创建的唯一绝对语句期限
     * @return 不携带行结果、包含当前事务状态的命令结果
     */
    private CommandResult executeSavepoint(
            SavepointStatementNode savepoint, SqlStatementDeadline deadline) {
        // 1、名称保留用户拼写；规范化只由 transaction policy 完成。
        String name = savepoint.name().value();
        // 2、所有 gateway 等待继续消费同一个 deadline。
        switch (savepoint.kind()) {
            // 3、每个分支都由 policy 在 storage 成功后才更新 Session 名称映射。
            case CREATE -> transactions.createSavepoint(name, deadline);
            case ROLLBACK_TO -> transactions.rollbackToSavepoint(name, deadline);
            case RELEASE -> transactions.releaseSavepoint(name, deadline);
        }
        // 4、保存点不改变 autocommit/active 标志，只观察当前状态。
        return new CommandResult(transactions.status());
    }

    /**
     * 分派 XA 控制语句。活动 branch 生命周期由 transaction policy 拥有；持久 PREPARED 与 phase two
     * 由实例 coordinator 拥有，Session 不读取 registry channel 或 storage Transaction。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>把 AST XID 转换为防御性复制的领域 XaId；RECOVER 不要求 XID。</li>
     *     <li>按命令调用 transaction policy，并把同一 statement deadline 的剩余预算传给持久 phase。</li>
     *     <li>RECOVER 将 registry PREPARED 快照投影为四列公开结果，不暴露 opaque handle。</li>
     *     <li>其它命令返回最新 transaction status；PREPARE 成功后 active=false。</li>
     * </ol>
     *
     * @param xa Parser 已交叉校验的 XA AST
     * @param deadline 当前 execute 唯一绝对期限
     * @return XA RECOVER 查询结果或命令结果
     */
    private SqlExecutionResult executeXa(XaStatementNode xa, SqlStatementDeadline deadline) {
        // 1、RECOVER 是唯一没有 XID 的命令。
        if (xa.kind() == XaStatementNode.Kind.RECOVER) {
            // 3、结果列对齐 MySQL XA RECOVER 的 format/gtrid/bqual/data 形态。
            return xaRecoverResult(xa.convertXid());
        }
        var xid = xa.xid().orElseThrow().toXaId();
        // 2、JOIN/RESUME 不区分运行期行为；两者都只能恢复本 Session ended branch。
        switch (xa.kind()) {
            case START -> transactions.startXa(
                    xid, xa.startMode() != XaStatementNode.StartMode.NONE);
            case END -> transactions.endXa(xid,
                    xa.endMode() != XaStatementNode.EndMode.NONE,
                    xa.endMode() == XaStatementNode.EndMode.FOR_MIGRATE);
            case PREPARE -> transactions.prepareXa(
                    xid, deadline.remaining("XA PREPARE transition"));
            case COMMIT -> transactions.commitXa(
                    xid, xa.onePhase(), deadline.remaining("XA COMMIT transition"));
            case ROLLBACK -> transactions.rollbackXa(
                    xid, deadline.remaining("XA ROLLBACK transition"));
            case RECOVER -> throw new SessionStateException("XA RECOVER dispatch invariant violated");
        }
        // 4、命令结果只发布 Session 事务快照；storage/registry identity 不泄漏。
        return new CommandResult(transactions.status());
    }

    /**
     * 把 durable PREPARED registry 快照转换为公开 QueryResult。
     */
    private QueryResult xaRecoverResult(boolean convertXid) {
        ColumnTypeDefinition integer = ColumnTypeDefinition.bigint(false, false);
        ColumnTypeDefinition dataType = ColumnTypeDefinition.scalar(
                convertXid ? DictionaryTypeId.VARCHAR : DictionaryTypeId.VARBINARY,
                false, false);
        List<ResultColumn> columns = List.of(
                new ResultColumn("formatID", integer),
                new ResultColumn("gtrid_length", integer),
                new ResultColumn("bqual_length", integer),
                new ResultColumn("data", dataType));
        ArrayList<SqlRow> rows = new ArrayList<>();
        for (var entry : transactions.recoverXa()) {
            byte[] gtrid = entry.xid().gtrid();
            byte[] bqual = entry.xid().bqual();
            byte[] combined = new byte[gtrid.length + bqual.length];
            System.arraycopy(gtrid, 0, combined, 0, gtrid.length);
            System.arraycopy(bqual, 0, combined, gtrid.length, bqual.length);
            SqlValue data = convertXid
                    ? new SqlValue.StringValue(entry.xid().toString())
                    : new SqlValue.BytesValue(combined);
            rows.add(new SqlRow(List.of(
                    new SqlValue.IntegerValue(BigInteger.valueOf(entry.xid().formatId())),
                    new SqlValue.IntegerValue(BigInteger.valueOf(gtrid.length)),
                    new SqlValue.IntegerValue(BigInteger.valueOf(bqual.length)),
                    data)));
        }
        return new QueryResult(columns, rows, transactions.status());
    }

    /** 快照也使用有界锁，避免读取 transaction policy 的一半状态转换。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
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

    /** close 与 execute 公平竞争；full rollback 后关闭 metadata，deregister 始终幂等且 close 异常保留。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     */
    @Override
    public void close() {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        if (state == SessionState.CLOSED) return;
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        SqlStatementDeadline deadline = SqlStatementDeadline.after(options.statementTimeout());
        acquire(deadline, "close");
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        if (failure != null) throw failure;
    }

    /**
     * 按会话与事务边界并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param operation 传给 {@code acquire} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws SessionBusyException SQL 绑定、会话准入或事务结果无法按当前状态完成时抛出；调用方应报告错误并按事务边界回滚或关闭
     */
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
                && control.kind() == TransactionControlNode.Kind.ROLLBACK
                || statement instanceof XaStatementNode xa
                && xa.kind() == XaStatementNode.Kind.ROLLBACK;
    }

    private static SqlExecutionResult withStatus(SqlExecutionResult result, TransactionStatus status) {
        return switch (result) {
            case QueryResult query -> new QueryResult(query.columns(), query.rows(), status);
            case UpdateResult update -> new UpdateResult(update.affectedRows(), status);
            case CommandResult ignored -> new CommandResult(status);
        };
    }

}
