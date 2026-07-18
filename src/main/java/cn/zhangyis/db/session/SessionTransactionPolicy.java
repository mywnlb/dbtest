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
    /**
     * 构造时冻结的 {@code options} 配置快照；已完成范围和组合校验，运行期策略读取它但不得就地修改。
     */
    private final SessionOptions options;
    /**
     * 本对象持有的 {@code gateway} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SqlStorageGateway gateway;
    /**
     * 本对象持有的 {@code dictionary} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DataDictionaryService dictionary;
    /**
     * 构造时冻结的 {@code owner} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final MdlOwnerId owner;
    /**
     * 冻结或发布的 {@code autocommit} 领域属性；该标志决定 NULL、数值符号、索引唯一性或事务访问分支，不能与 schema/会话状态矛盾。
     */
    private boolean autocommit;
    /**
     * 本对象关联的 {@code active} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
     */
    private ActiveTransaction active;
    /**
     * 记录 {@code rollbackOnly} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private boolean rollbackOnly;
    /**
     * 记录 {@code closed} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private boolean closed;

    /**
     * 创建 {@code SessionTransactionPolicy}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param options 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param gateway 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param dictionary 由组合根提供的 {@code DataDictionaryService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param owner 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SessionTransactionPolicy(SessionOptions options, SqlStorageGateway gateway,
                                    DataDictionaryService dictionary, MdlOwnerId owner) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (options == null || gateway == null || dictionary == null || owner == null) {
            throw new DatabaseValidationException("transaction policy collaborators must not be null");
        }
        this.options = options;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.gateway = gateway;
        this.dictionary = dictionary;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.owner = owner;
        this.autocommit = options.autocommit();
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        if (!autocommit) active = begin(false, SessionTransactionMode.IMPLICIT);
    }

    /** 普通语句准入：autocommit 创建临时 RO/RW 事务；autocommit=0 的缺失 implicit 状态会被自愈创建。
     *
     * @param readOnly 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void prepareData(boolean readOnly) {
        ensureOpen();
        if (rollbackOnly) throw new SessionStateException("rollback-only transaction accepts only ROLLBACK/close");
        if (active == null) {
            active = begin(autocommit && readOnly,
                    autocommit ? SessionTransactionMode.AUTOCOMMIT_STATEMENT : SessionTransactionMode.IMPLICIT);
        }
    }

    /** 当前 active transaction 的 metadata statement staging scope。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code beginBinding} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     */
    public StatementBindingScope beginBinding(Duration timeout) {
        requireActive();
        return active.metadata.beginStatement(timeout);
    }

    /** 当前 active opaque handle；只交给 SQL executor。
     *
     * @return {@code handle} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public SqlTransactionHandle handle() {
        requireActive();
        return active.handle;
    }

    /** autocommit 普通语句成功后提交临时事务。
     *
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     */
    public void completeAutocommit(Duration remaining) {
        requireMode(SessionTransactionMode.AUTOCOMMIT_STATEMENT);
        finishCommit(remaining);
    }

    /** autocommit 普通语句失败后完整回滚临时事务。 */
    public void failAutocommit() {
        requireMode(SessionTransactionMode.AUTOCOMMIT_STATEMENT);
        finishRollback();
    }

    /** BEGIN：已有 implicit/explicit 先隐式提交，再创建新的 explicit transaction。
     *
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void beginExplicit(Duration remaining) {
        ensureOpen();
        if (rollbackOnly) throw new SessionStateException("rollback-only transaction cannot BEGIN");
        if (active != null) finishCommit(remaining);
        active = begin(false, SessionTransactionMode.EXPLICIT);
    }

    /** SET autocommit 按设计先完成所需 storage 动作，再发布对用户可见变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param enabled 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void setAutocommit(boolean enabled, Duration remaining) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        ensureOpen();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (enabled == autocommit) return;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (enabled) {
            if (rollbackOnly) throw new SessionStateException("rollback-only transaction cannot enable autocommit");
            if (active != null) finishCommit(remaining);
            autocommit = true;
            return;
        }
        // 变量先临时翻转；begin 失败恢复，调用方永远看不到无 implicit transaction 的 autocommit=false 发布态。
        autocommit = false;
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            active = begin(false, SessionTransactionMode.IMPLICIT);
        } catch (RuntimeException beginFailure) {
            autocommit = true;
            throw beginFailure;
        }
    }

    /** COMMIT；autocommit=false 成功终结后立即创建新的 implicit transaction。NONE 为 no-op。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void commitAndContinue(Duration remaining) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        ensureOpen();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (active == null) return;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (rollbackOnly) throw new SessionStateException("rollback-only transaction cannot COMMIT");
        finishCommit(remaining);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        if (!autocommit) active = begin(false, SessionTransactionMode.IMPLICIT);
    }

    /** ROLLBACK；autocommit=false 成功终结后立即创建新的 implicit transaction。NONE 为 no-op。
     *
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     */
    public void rollbackAndContinue(Duration remaining) {
        ensureOpen();
        if (active == null) return;
        finishRollback();
        if (!autocommit) active = begin(false, SessionTransactionMode.IMPLICIT);
    }

    /**
     * DDL 前执行 MySQL 风格 implicit commit。storage 终态确认后 metadata scope 才关闭，因此 table X
     * 不会与 Session 自身 transaction-duration MDL 形成自锁。
     *
     * @param remaining 当前 statement deadline 剩余预算，用于可能发生的 commit durability
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void prepareDdl(Duration remaining) {
        ensureOpen();
        if (rollbackOnly) {
            throw new SessionStateException("rollback-only transaction cannot execute DDL");
        }
        if (active != null) {
            finishCommit(remaining);
        }
    }

    /**
     * DDL 结束后恢复 autocommit=0 的隐式事务语义；DDL 自身不属于该新事务。
     */
    public void resumeAfterDdl() {
        ensureOpen();
        if (!autocommit && active == null) {
            active = begin(false, SessionTransactionMode.IMPLICIT);
        }
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

    /**
     * 校验当前状态后推进会话与事务边界状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     */
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

    /**
     * 校验当前状态后推进会话与事务边界状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     */
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

    /**
     * 校验当前状态后推进会话与事务边界状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     */
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

    /** active transaction 的双 owner 聚合；只有 policy 能替换/清除。
     *
     * @param handle 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param metadata 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     */
    private record ActiveTransaction(SqlTransactionHandle handle, TransactionMetadataScope metadata,
                                     SessionTransactionMode mode) { }
}
