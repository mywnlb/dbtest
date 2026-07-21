package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseFailureClassifier;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.BoundRangeSelect;
import cn.zhangyis.db.sql.binder.bound.BoundRangeUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundRangeDelete;
import cn.zhangyis.db.sql.binder.bound.BoundIndexRange;
import cn.zhangyis.db.sql.binder.bound.BoundRangeEndpoint;
import cn.zhangyis.db.sql.binder.bound.BoundRowPredicate;
import cn.zhangyis.db.sql.binder.bound.BoundRowPredicateOperator;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.binder.bound.PointAccessKind;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.*;
import cn.zhangyis.db.storage.api.dml.*;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadMode;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeScanRange;
import cn.zhangyis.db.storage.btree.SearchKeyComparator;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.engine.EngineState;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.engine.RecoveryExportWriteRejectedException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.trx.*;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * SQL port 的唯一 storage adapter。输入 exact TableDefinition 只经 mapper 派生一次；事务、ReadView、MTR、LOB
 * hydration 和 DML statement guard 均留在本包，SQL 层不会接触物理类型。
 */
public final class DefaultSqlStorageGateway implements SqlStorageGateway {
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏SQL 与存储引擎适配层的不变量。
     */
    private static final BigInteger TWO = BigInteger.valueOf(2);
    /** 单个只读 MTR/current-read 定位批次，限制 page latch/fix 的驻留工作量。 */
    private static final int RANGE_SCAN_BATCH_SIZE = 256;
    /** SQL 一条 range 语句最多发布的聚簇 identity 数；多取一行仅用于 fail-closed 检测。 */
    private static final int RANGE_ROW_LIMIT = 4096;
    /** marked secondary history 等物理放大最多允许检查的 candidate 数。 */
    private static final int RANGE_PHYSICAL_CANDIDATE_LIMIT = 16_384;
    /** current-read 等待后重定位上限，与既有 secondary locking reader 保持一致。 */
    private static final int RANGE_RELOCATION_RETRIES = 3;
    /**
     * 本对象持有的 {@code engine} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final StorageEngine engine;
    /**
     * 本对象拥有的 {@code mapper} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final DictionaryStorageMetadataMapper mapper;
    /** 行锁等待与 handle 并发占用都必须有界；Session 可用 statement timeout 构造此 adapter。 */
    private final Duration operationTimeout;
    /** FORCE 导出模式的 gateway 级二次防线；即使绕过 Session AST 也不能写 storage。 */
    private final boolean recoveryExportReadOnly;
    /** residual comparison 复用 Record 的 NULL/type/collation 排序，不在 SQL adapter 复制比较规则。 */
    private final SearchKeyComparator predicateComparator =
            new SearchKeyComparator(new TypeCodecRegistry());

    /**
     * 创建 {@code DefaultSqlStorageGateway}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param engine 由组合根提供的 {@code StorageEngine} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mapper 参与 {@code 构造} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @param operationTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlStorageGateway(StorageEngine engine, DictionaryStorageMetadataMapper mapper,
                                    Duration operationTimeout) {
        this(engine, mapper, operationTimeout, false);
    }

    /**
     * @param engine 当前已打开 storage 组合根
     * @param mapper exact DD-to-storage mapper
     * @param operationTimeout handle/lock 等待上限
     * @param recoveryExportReadOnly 是否启用 FORCE 导出写拒绝
     */
    public DefaultSqlStorageGateway(StorageEngine engine, DictionaryStorageMetadataMapper mapper,
                                    Duration operationTimeout, boolean recoveryExportReadOnly) {
        if (engine == null || mapper == null || operationTimeout == null
                || operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new DatabaseValidationException("gateway engine/mapper/positive operation timeout required");
        }
        this.engine = engine;
        this.mapper = mapper;
        this.operationTimeout = operationTimeout;
        this.recoveryExportReadOnly = recoveryExportReadOnly;
    }

    /** 显式映射 SQL isolation；v1 只开放已有 RR/RC ReadView 语义。
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code begin} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public SqlTransactionHandle begin(SqlTransactionRequest request) {
        if (request == null) throw new DatabaseValidationException("SQL transaction request must not be null");
        requireEngineOpen();
        IsolationLevel isolation = switch (request.isolationLevel()) {
            case READ_UNCOMMITTED -> IsolationLevel.READ_UNCOMMITTED;
            case READ_COMMITTED -> IsolationLevel.READ_COMMITTED;
            case REPEATABLE_READ -> IsolationLevel.REPEATABLE_READ;
            // 单语句 autocommit 普通 SELECT 保持一致性读；显式 locking SELECT 即使映射 RC 仍走 current-read。
            case SERIALIZABLE -> request.autocommit()
                    ? IsolationLevel.READ_COMMITTED : IsolationLevel.SERIALIZABLE;
        };
        try {
            Transaction transaction = engine.transactionManager().begin(
                    new TransactionOptions(isolation, request.readOnly(), request.autocommit()));
            return new EngineSqlTransactionHandle(this, transaction);
        } catch (RuntimeException error) {
            throw adapt("begin SQL transaction failed", error);
        }
    }

    /**
     * 在 registry PREPARING 前读取 branch 身份。真实写入已经在 DML 首写时分配 transaction id；
     * 本方法不为了 XA 人为制造无 undo 的写者，保证只读 prepare 可以走 READ_ONLY。
     *
     * @param transaction 本 adapter 创建且仍为 ACTIVE 的 opaque handle
     * @return 有 undo/成功写入时返回正 id，否则返回只读身份
     */
    @Override
    public SqlXaTransactionIdentity xaIdentity(SqlTransactionHandle transaction) {
        rejectRecoveryExportWrite("XA identity/prepare");
        return withActive(transaction, null, handle -> {
            boolean hasWrites = handle.wrote || handle.transaction.undoContext() != null;
            long transactionId = hasWrites ? handle.transaction.transactionId().value() : 0;
            if (hasWrites && transactionId <= 0) {
                throw new SqlTransactionStateException(
                        "XA write branch has no assigned storage transaction id");
            }
            return new SqlXaTransactionIdentity(transactionId, hasWrites);
        });
    }

    /**
     * 调用 storage prepared facade 完成 phase one。成功后 handle 进入 PREPARED 并拒绝普通 SQL；
     * 失败若 storage 已发布 PREPARED，也保留 PREPARED 使协调器只能恢复/重试，不能普通回滚。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>有界取得 opaque handle owner 锁并确认 ACTIVE、正 timeout 与真实写身份。</li>
     *     <li>调用 PreparedTransactionService，使 undo PREPARED 与 phase-one redo 强持久。</li>
     *     <li>成功发布 handle PREPARED，并返回 transaction id/LSN。</li>
     *     <li>异常时读取 storage 状态；若已越过 PREPARED 边界则同步发布 PREPARED，保留同方向恢复能力。</li>
     * </ol>
     *
     * @param transaction 本 adapter 创建的 ACTIVE 写事务能力
     * @param timeout phase-one durability 正等待上限
     * @return PREPARED durable 结果
     */
    @Override
    public SqlXaPrepareOutcome prepareXa(SqlTransactionHandle transaction, Duration timeout) {
        rejectRecoveryExportWrite("XA prepare");
        requirePositiveXaTimeout(timeout, "prepare");
        return withActive(transaction, null, handle -> {
            // 1、只有已有真实 write/undo owner 的 branch 才进入 storage PREPARED。
            if ((!handle.wrote && handle.transaction.undoContext() == null)
                    || handle.transaction.transactionId().isNone()) {
                throw new SqlTransactionStateException("XA prepare requires a written transaction");
            }
            try {
                // 2、storage facade 在返回前强制 phase-one redo durable，并保留全部事务锁。
                var prepared = engine.preparedTransactionService().prepare(
                        new cn.zhangyis.db.storage.api.trx.PrepareTransactionCommand(
                                handle.transaction, timeout));
                // 3、opaque 状态只在物理 participant 已发布后推进。
                handle.state = EngineSqlTransactionHandle.State.PREPARED;
                return new SqlXaPrepareOutcome(
                        prepared.transactionId().value(), prepared.durableLsn().value());
            } catch (RuntimeException failure) {
                // 4、durability 响应失败也可能已经发布 PREPARED；禁止调用方改走普通事务终结。
                if (handle.transaction.state() == TransactionState.PREPARED) {
                    handle.state = EngineSqlTransactionHandle.State.PREPARED;
                }
                throw adapt("XA prepare failed", failure);
            }
        });
    }

    /**
     * 按 durable commit decision 完成 prepared phase two。COMMIT_DECIDED 保留到 storage
     * terminal+fsync+lock release 全部成功，任何失败只允许同方向重试。
     */
    @Override
    public SqlXaCompletionOutcome commitPreparedXa(
            SqlTransactionHandle transaction, Duration timeout) {
        rejectRecoveryExportWrite("XA prepared commit");
        requirePositiveXaTimeout(timeout, "commit prepared");
        return withXaState(transaction, true, handle -> {
            handle.state = EngineSqlTransactionHandle.State.COMMIT_DECIDED;
            try {
                var completed = engine.preparedTransactionService().commitPrepared(
                        new cn.zhangyis.db.storage.api.trx.CommitPreparedTransactionCommand(
                                handle.transaction, timeout));
                handle.state = EngineSqlTransactionHandle.State.COMMITTED;
                return new SqlXaCompletionOutcome(completed.transactionId().value(), true,
                        completed.transactionNo().value(), completed.durableLsn().value(),
                        completed.releasedLockCount(), completed.undoRecordsApplied());
            } catch (RuntimeException failure) {
                throw adapt("XA prepared commit failed; commit decision remains authoritative", failure);
            }
        });
    }

    /**
     * 按 durable rollback decision 完成 prepared phase two。ROLLBACK_DECIDED 保留到 storage
     * terminal+fsync+lock release 全部成功，任何失败只允许同方向重试。
     */
    @Override
    public SqlXaCompletionOutcome rollbackPreparedXa(
            SqlTransactionHandle transaction, Duration timeout) {
        rejectRecoveryExportWrite("XA prepared rollback");
        requirePositiveXaTimeout(timeout, "rollback prepared");
        return withXaState(transaction, false, handle -> {
            handle.state = EngineSqlTransactionHandle.State.ROLLBACK_DECIDED;
            try {
                var completed = engine.preparedTransactionService().rollbackPrepared(
                        new cn.zhangyis.db.storage.api.trx.ResolvedRollbackPreparedTransactionCommand(
                                handle.transaction, timeout));
                handle.state = EngineSqlTransactionHandle.State.ROLLED_BACK;
                return new SqlXaCompletionOutcome(completed.transactionId().value(), false,
                        completed.transactionNo().value(), completed.durableLsn().value(),
                        completed.releasedLockCount(), completed.undoRecordsApplied());
            } catch (RuntimeException failure) {
                throw adapt("XA prepared rollback failed; rollback decision remains authoritative", failure);
            }
        });
    }

    /**
     * 创建同时覆盖 undo 与锁获取序号的不透明 SQL 保存点。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在 statement deadline 内独占并复核 ACTIVE transaction handle。</li>
     *     <li>先捕获 LockManager 请求高水位；该动作不取得页 latch 或分配事务写 id。</li>
     *     <li>按事务是否已有 undo context 创建空边界或双 logical-head 保存点。</li>
     *     <li>把两类边界封装为 adapter 私有能力后返回，Session 不接触 storage 对象。</li>
     * </ol>
     *
     * @param transaction 本 gateway 创建且仍 ACTIVE 的事务能力
     * @param deadline 当前 SQL 语句绝对期限
     * @return 同时绑定 undo/lock 边界的不透明保存点能力
     */
    @Override
    public SqlSavepointHandle createSavepoint(SqlTransactionHandle transaction,
                                              SqlStatementDeadline deadline) {
        if (deadline == null) {
            throw new DatabaseValidationException("savepoint deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            var lockBoundary = engine.lockManager().createSavepoint();
            if (handle.transaction.undoContext() == null) {
                EmptyUndoBoundary empty = engine.rollbackService()
                        .createEmptyStatementBoundary(handle.transaction);
                return EngineSqlSavepointHandle.empty(
                        this, handle.transaction, empty, lockBoundary);
            }
            TransactionSavepoint undo = engine.rollbackService()
                    .createSavepoint(handle.transaction);
            return EngineSqlSavepointHandle.undo(
                    this, handle.transaction, undo, lockBoundary);
        });
    }

    /**
     * 先完成 undo 反向应用，再按 retention 白名单释放保存点后锁；普通事务锁始终保留。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在事务 handle 锁内校验保存点 owner、transaction identity 与 OPEN 生命周期。</li>
     *     <li>使用 DD target resolver 回滚目标之后的全部 undo；此阶段不释放事务锁。</li>
     *     <li>若事务已有真实写 id，按锁获取序号只释放明确可提前释放的请求。</li>
     *     <li>UNDO_SAVEPOINT 保留原能力；EMPTY_UNDO 被消费后创建等价新边界并关闭旧能力。</li>
     * </ol>
     *
     * @param transaction 保存点所属 ACTIVE 事务
     * @param savepoint 本 gateway 创建且尚未释放的能力
     * @param deadline 当前 SQL 语句绝对期限
     * @return 回滚后仍有效的目标保存点能力
     */
    @Override
    public SqlSavepointHandle rollbackToSavepoint(
            SqlTransactionHandle transaction, SqlSavepointHandle savepoint,
            SqlStatementDeadline deadline) {
        if (deadline == null) {
            throw new DatabaseValidationException("rollback-to-savepoint deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            EngineSqlSavepointHandle boundary = requireSavepoint(handle, savepoint);
            try {
                if (boundary.boundaryKind
                        == EngineSqlSavepointHandle.BoundaryKind.EMPTY_UNDO) {
                    engine.rollbackService().rollbackToEmptyStatementBoundary(
                            handle.transaction, boundary.emptyBoundary);
                } else {
                    engine.rollbackService().rollbackToSavepoint(
                            handle.transaction, boundary.undoSavepoint);
                }
                if (!handle.transaction.transactionId().isNone()) {
                    engine.lockManager().rollbackToSavepoint(
                            handle.transaction.transactionId(), boundary.lockSavepoint);
                }
                if (boundary.boundaryKind
                        == EngineSqlSavepointHandle.BoundaryKind.UNDO_SAVEPOINT) {
                    return boundary;
                }
                boundary.open = false;
                var replacementLocks = engine.lockManager().createSavepoint();
                if (handle.transaction.undoContext() == null) {
                    return EngineSqlSavepointHandle.empty(this, handle.transaction,
                            engine.rollbackService().createEmptyStatementBoundary(handle.transaction),
                            replacementLocks);
                }
                return EngineSqlSavepointHandle.undo(this, handle.transaction,
                        engine.rollbackService().createSavepoint(handle.transaction),
                        replacementLocks);
            } catch (RuntimeException rollbackFailure) {
                throw adapt("rollback to SQL savepoint failed", rollbackFailure);
            }
        });
    }

    /**
     * 仅释放目标 runtime 保存点；更晚保存点、undo 链和事务锁均保持原状。
     *
     * @param transaction 保存点所属 ACTIVE 事务
     * @param savepoint 本 gateway 创建且尚未释放的能力
     * @param deadline 当前 SQL 语句绝对期限
     */
    @Override
    public void releaseSavepoint(SqlTransactionHandle transaction, SqlSavepointHandle savepoint,
                                 SqlStatementDeadline deadline) {
        if (deadline == null) {
            throw new DatabaseValidationException("release-savepoint deadline must not be null");
        }
        withActive(transaction, deadline, handle -> {
            EngineSqlSavepointHandle boundary = requireSavepoint(handle, savepoint);
            if (boundary.boundaryKind == EngineSqlSavepointHandle.BoundaryKind.EMPTY_UNDO) {
                engine.rollbackService().releaseEmptyStatementBoundary(
                        handle.transaction, boundary.emptyBoundary);
            } else {
                engine.rollbackService().releaseSavepoint(
                        handle.transaction, boundary.undoSavepoint);
            }
            boundary.open = false;
            return null;
        });
    }

    /**
     * 完整行先映射并由 Record codec 在 DML 中最终复核；statement guard 覆盖实际写入和成功 close。任一失败先执行
     * partial rollback，若 rollback 本身失败则保留原始异常并显式报告 rollback-only。
     * <ol>
     *     <li>以 statement deadline 限制 opaque handle 准入，并从 exact-version DD definition 映射物理 index/LOB binding。</li>
     *     <li>把 SQL 值转换成 record 值并派生聚簇 key；转换失败早于 undo、LOB、row 或锁副作用。</li>
     *     <li>创建 statement guard 后调用 DML facade，row-lock timeout 取配置与 deadline 剩余值的较小者。</li>
     *     <li>成功关闭 guard 并发布 handle wrote；失败由 guard 做 partial rollback，异常保留 rollback-only/fatal 语义。</li>
     * </ol>
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code insert} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement,
                                  SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("INSERT");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound INSERT/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1. mapper 只消费 binder 固定的 DD version，不重新按表名读取可能已变化的 metadata。
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex index = mapped.clusteredIndex();
            // 2. record/schema 校验仍在实际业务 MTR 前完成，key 顺序由 index definition 权威决定。
            List<ColumnValue> values = toColumnValues(statement.values(), index);
            LogicalRecord record = new LogicalRecord(index.schema().schemaVersion(), values, false,
                    RecordType.CONVENTIONAL);
            // 3. guard 快照双 undo head；DML 成功前不发布 statement terminal。
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, index);
            try {
                DmlWriteResult result = engine.tableDmlService().insert(new TableInsertCommand(handle.transaction,
                        mapped.tableIndexes(), record, mapped.lobSegment(),
                        deadline.cap(operationTimeout, "clustered INSERT row-lock wait")));
                // 4. guard.close 成功才允许 handle 记为已写；失败路径由 catch 汇总 partial rollback 结果。
                guard.close();
                handle.wrote |= result.changed();
                return new SqlWriteOutcome(result.affectedRows(), handle.transaction.rollbackOnly());
            } catch (RuntimeException writeFailure) {
                try {
                    guard.rollback();
                } catch (RuntimeException rollbackFailure) {
                    writeFailure.addSuppressed(rollbackFailure);
                    throw new SqlStatementRollbackException("INSERT failed and statement rollback was not confirmed",
                            handle.transaction.rollbackOnly(), writeFailure);
                }
                throw adapt("clustered INSERT failed after confirmed statement rollback", writeFailure);
            }
        });
    }

    /** 主键点 UPDATE：在 storage 的 FOR_UPDATE 锁定版本上应用 typed patch，避免 gateway 先读后写竞态。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code update} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public SqlWriteOutcome update(SqlTransactionHandle transaction, BoundUpdate statement,
                                  SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("UPDATE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound UPDATE/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex index = mapped.clusteredIndex();
            SearchKey key = new SearchKey(toKeyValues(statement.primaryKeyValues(), index));
            ArrayList<TableColumnAssignment> assignments = new ArrayList<>(statement.assignmentOrdinals().size());
            for (int i = 0; i < statement.assignmentOrdinals().size(); i++) {
                int ordinal = statement.assignmentOrdinals().get(i);
                assignments.add(new TableColumnAssignment(ordinal,
                        toColumnValue(statement.assignmentValues().get(i), index.schema().column(ordinal).type())));
            }
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, index);
            try {
                DmlWriteResult result = engine.tableDmlService().update(new TableUpdatePatchCommand(
                        handle.transaction, mapped.tableIndexes(), key, assignments, mapped.lobSegment(),
                        deadline.cap(operationTimeout, "point UPDATE row-lock wait")));
                guard.close();
                handle.wrote |= result.changed();
                return new SqlWriteOutcome(result.affectedRows(), handle.transaction.rollbackOnly());
            } catch (RuntimeException writeFailure) {
                try {
                    guard.rollback();
                } catch (RuntimeException rollbackFailure) {
                    writeFailure.addSuppressed(rollbackFailure);
                    throw new SqlStatementRollbackException(
                            "UPDATE failed and statement rollback was not confirmed",
                            handle.transaction.rollbackOnly(), writeFailure);
                }
                throw adapt("point UPDATE failed after confirmed statement rollback", writeFailure);
            }
        });
    }

    /** 主键点 DELETE：LOB binding 与 exact-version 多索引 metadata 一并交给表级 DML。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code delete} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public SqlWriteOutcome delete(SqlTransactionHandle transaction, BoundDelete statement,
                                  SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("DELETE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound DELETE/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex index = mapped.clusteredIndex();
            SearchKey key = new SearchKey(toKeyValues(statement.primaryKeyValues(), index));
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, index);
            try {
                DmlWriteResult result = engine.tableDmlService().delete(new TableDeleteCommand(
                        handle.transaction, mapped.tableIndexes(), key, mapped.lobSegment(),
                        deadline.cap(operationTimeout, "point DELETE row-lock wait")));
                guard.close();
                handle.wrote |= result.changed();
                return new SqlWriteOutcome(result.affectedRows(), handle.transaction.rollbackOnly());
            } catch (RuntimeException writeFailure) {
                try {
                    guard.rollback();
                } catch (RuntimeException rollbackFailure) {
                    writeFailure.addSuppressed(rollbackFailure);
                    throw new SqlStatementRollbackException(
                            "DELETE failed and statement rollback was not confirmed",
                            handle.transaction.rollbackOnly(), writeFailure);
                }
                throw adapt("point DELETE failed after confirmed statement rollback", writeFailure);
            }
        });
    }

    /**
     * 聚簇主键点查：RR 复用事务 ReadView，RC 每语句 finally 注销；整行 LOB hydrate 完成后才投影。
     * <ol>
     *     <li>在 deadline 内进入 handle，并把 exact DD binding 映射为 index/key。</li>
     *     <li>创建隔离级别对应的 ReadView；RC view 立即登记到 purge low-water 集合。</li>
     *     <li>执行 MVCC 点查，并在同一个 view 存活期间用短 MTR hydrate 所有 external LOB。</li>
     *     <li>完整行 hydration 后才按 projection ordinal 转换公开 SqlValue，避免返回 partial row/reference。</li>
     *     <li>finally 注销 RC view；读取失败为主异常，close 失败作为 suppressed，fatal 严重度不得降级。</li>
     * </ol>
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code selectPoint} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public Optional<SqlRow> selectPoint(SqlTransactionHandle transaction, BoundPointSelect statement,
                                        SqlStatementDeadline deadline) {
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound SELECT/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1. handle wait 已由 withActive 截断，下面所有 LOB 阶段继续消费同一绝对 deadline。
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex clusteredIndex = mapped.clusteredIndex();
            BTreeIndex accessIndex = mapped.index(statement.accessIndexId());
            SearchKey key = new SearchKey(toKeyValues(statement.keyValues(), accessIndex));
            if (statement.keyValues().stream().anyMatch(SqlValue.NullValue.class::isInstance)) {
                return Optional.empty();
            }
            if (handle.transaction.isolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
                List<LogicalRecord> records;
                if (statement.accessKind() == PointAccessKind.CLUSTERED_PRIMARY) {
                    records = engine.mvccReader().readUncommitted(clusteredIndex, key)
                            .map(List::of).orElseGet(List::of);
                } else {
                    records = scanReadUncommittedBatches(mapped, statement.accessIndexId(),
                            equalityRange(statement.keyValues()),
                            equalityPredicates(statement.table(), statement.accessIndexId(),
                                    statement.keyValues()),
                            deadline);
                    if (records.size() > 1) {
                        throw new SqlStorageException(
                                "multiple current rows for logical unique secondary key");
                    }
                }
                List<SqlRow> projected = projectRows(records, statement.projectionOrdinals(),
                        statement.table(), clusteredIndex, deadline);
                return projected.stream().findFirst();
            }
            ReadViewManager views = engine.transactionManager().readViewManager();
            deadline.remaining("point-select ReadView creation");
            // 2. TransactionSystem 在此登记 live view，purge 在 closeReadView 前不能越过它。
            ReadView view = views.openReadView(handle.transaction);
            RuntimeException failure = null;
            try {
                // 3. view 覆盖版本选择与外置引用解引用，避免 RC 提前注销后 purge 回收旧 LOB ownership。
                Optional<LogicalRecord> record = statement.accessKind() == PointAccessKind.CLUSTERED_PRIMARY
                        ? engine.mvccReader().read(view, clusteredIndex, key)
                        : engine.secondaryMvccReader().readUnique(view, mapped.tableIndexes(),
                                mapped.tableIndexes().requireSecondary(statement.accessIndexId()), key);
                if (record.isEmpty()) return Optional.empty();

                // RC view 必须覆盖 external chain hydration；否则 purge low water 可能在引用解引用前越过该版本。
                List<ColumnValue> hydrated = hydrateExternalValues(
                        record.orElseThrow().columnValues(), clusteredIndex, deadline);
                // 4. hydration 全部成功后才构造公开结果，storage LobReference 永不越过 gateway。
                List<SqlValue> projected = new ArrayList<>(statement.projectionOrdinals().size());
                for (int ordinal : statement.projectionOrdinals()) {
                    projected.add(toSqlValue(hydrated.get(ordinal), statement.table().columns().get(ordinal).type()));
                }
                return Optional.of(new SqlRow(projected));
            } catch (RuntimeException readFailure) {
                failure = adapt("point-select MVCC/LOB read failed", readFailure);
                throw failure;
            } finally {
                // 5. RR 由事务终态 release；RC 必须在全部读取工作结束后恰好注销，且不能覆盖主失败。
                if (handle.transaction.isolationLevel() == IsolationLevel.READ_COMMITTED) {
                    try {
                        views.closeReadView(view);
                    } catch (RuntimeException closeFailure) {
                        RuntimeException adapted = adapt("close READ COMMITTED ReadView failed", closeFailure);
                        if (failure == null) throw adapted;
                        failure.addSuppressed(adapted);
                    }
                }
            }
        });
    }

    /**
     * 执行 non-unique secondary logical-prefix range read。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在 statement deadline 内进入 opaque handle，并从 exact DD version 映射 secondary layout/key。</li>
     *     <li>consistent 模式创建 RC/RR ReadView，扫描 marked candidates 后回聚簇 MVCC；locking 模式改走
     *         logical-prefix S/X 与聚簇 current-read，不创建历史快照。</li>
     *     <li>对每条完整聚簇结果 hydrate external LOB，再按 Binder ordinal 构造公开 SqlRow。</li>
     *     <li>任何 candidate/LOB 失败都不发布 partial list；locking locks 保持到事务终态。</li>
     *     <li>RC 在全部行 hydration/投影后 finally 注销 ReadView；关闭失败保留主异常与 fatal 严重度。</li>
     * </ol>
     *
     * @param transaction 本 gateway 创建的 ACTIVE opaque handle。
     * @param statement   exact-version non-unique secondary range plan。
     * @param deadline    覆盖 handle、predicate/row lock 与 LOB hydration 的绝对语句期限。
     * @return 完整、不可变的多行公开结果；SQL NULL equality 返回空列表。
     * @throws DatabaseValidationException statement/deadline 缺失时抛出。
     * @throws SqlStorageException metadata 映射、MVCC/current-read、容量或 LOB 阶段失败时抛出并保留 cause。
     */
    @Override
    public List<SqlRow> selectRange(SqlTransactionHandle transaction, BoundSecondaryRangeSelect statement,
                                    SqlStatementDeadline deadline) {
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound range SELECT/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1. mapper 只消费 Binder 固定的 exact table version；logical key 不包含 storage clustered suffix。
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex clusteredIndex = mapped.clusteredIndex();
            BTreeIndex accessIndex = mapped.index(statement.accessIndexId());
            var secondary = mapped.tableIndexes().requireSecondary(statement.accessIndexId());
            SearchKey logicalKey = new SearchKey(toKeyValues(statement.logicalKeyValues(), accessIndex));
            if (statement.logicalKeyValues().stream().anyMatch(SqlValue.NullValue.class::isInstance)) {
                return List.of();
            }

            if (statement.lockMode() != SelectLockMode.CONSISTENT) {
                try {
                    // 2. locking read 只读当前版本；同一 absolute deadline 的剩余量成为 storage 内部共享等待预算。
                    List<LogicalRecord> records = engine.secondaryCurrentReadService().readRange(
                            handle.transaction, mapped.tableIndexes(), secondary, logicalKey,
                            statement.lockMode() == SelectLockMode.FOR_SHARE
                                    ? BTreeCurrentReadMode.FOR_SHARE : BTreeCurrentReadMode.FOR_UPDATE,
                            deadline.cap(operationTimeout, "secondary locking range"));
                    // 3/4. 全部行依次 hydrate/project；异常直接抛出，不返回已构造前缀。
                    return projectRows(records, statement.projectionOrdinals(), statement.table(),
                            clusteredIndex, deadline);
                } catch (RuntimeException readFailure) {
                    throw adapt("secondary locking range/LOB read failed", readFailure);
                }
            }

            if (handle.transaction.isolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
                List<LogicalRecord> records = scanReadUncommittedBatches(
                        mapped, statement.accessIndexId(),
                        equalityRange(statement.logicalKeyValues()),
                        equalityPredicates(statement.table(), statement.accessIndexId(),
                                statement.logicalKeyValues()),
                        deadline);
                return projectRows(records, statement.projectionOrdinals(), statement.table(),
                        clusteredIndex, deadline);
            }

            ReadViewManager views = engine.transactionManager().readViewManager();
            deadline.remaining("secondary range ReadView creation");
            // 2. consistent range 的一个 view 覆盖 secondary scan、逐候选 clustered/undo 与全部 LOB hydration。
            ReadView view = views.openReadView(handle.transaction);
            RuntimeException failure = null;
            try {
                List<LogicalRecord> records = engine.secondaryMvccReader().readRange(
                        view, mapped.tableIndexes(), secondary, logicalKey);
                // 3/4. projection 只有在每条 external value hydrate 完整后才创建，Storage reference 不越过 port。
                return projectRows(records, statement.projectionOrdinals(), statement.table(),
                        clusteredIndex, deadline);
            } catch (RuntimeException readFailure) {
                failure = adapt("secondary range MVCC/LOB read failed", readFailure);
                throw failure;
            } finally {
                // 5. RR view 由事务终态释放；RC 必须在最后一行投影完成后注销，且关闭失败不能覆盖主失败。
                if (handle.transaction.isolationLevel() == IsolationLevel.READ_COMMITTED) {
                    try {
                        views.closeReadView(view);
                    } catch (RuntimeException closeFailure) {
                        RuntimeException adapted = adapt("close READ COMMITTED range ReadView failed", closeFailure);
                        if (failure == null) {
                            throw adapted;
                        }
                        failure.addSuppressed(adapted);
                    }
                }
            }
        });
    }

    /**
     * 执行 comparison/composite/full-scan SELECT，并在 adapter 内收敛分页、MVCC/current-read、
     * residual、LOB hydration 与容量边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验计划并映射 exact DD version；empty plan 在创建 ReadView/事务锁前返回。</li>
     *     <li>以 256 条 physical candidate 为批次扫描；consistent read 复用一个 ReadView，
     *         locking read 每批短定位、释放页资源、等待锁并重定位。</li>
     *     <li>二级候选回聚簇取得可见/当前完整行，按聚簇 identity 去重，再用 Record 比较规则执行全部 residual。</li>
     *     <li>完整结果通过 4096 row 与 16384 physical candidate 双上限后，才 hydrate LOB 并发布公开行。</li>
     * </ol>
     *
     * @param transaction 本 gateway 创建且仍 ACTIVE 的不透明事务句柄
     * @param statement Binder 固定的 exact-version typed range plan
     * @param deadline 覆盖分页、锁等待、undo 与 LOB hydration 的绝对期限
     * @return 完整不可变结果；empty/无匹配时为空，不返回 partial prefix
     * @throws DatabaseValidationException 参数缺失时抛出
     * @throws SqlStorageException 扫描、回表、容量、锁或 LOB 阶段失败时抛出
     */
    @Override
    public List<SqlRow> selectRange(SqlTransactionHandle transaction, BoundRangeSelect statement,
                                    SqlStatementDeadline deadline) {
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound comparison range SELECT/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1、empty 是 Binder 的纯证明，不需要创建 view、分配 transaction id 或取得行锁。
            if (statement.empty()) {
                return List.of();
            }
            MappedTableStorage mapped = mapper.map(statement.table());
            if (statement.lockMode() == SelectLockMode.CONSISTENT) {
                // 2、同一个 view 覆盖全部分页、undo 与 residual；RC 只在所有 LOB 投影完成后关闭。
                return readConsistentRange(handle, mapped, statement, deadline);
            }
            List<LogicalRecord> records = readLockingRange(handle, mapped, statement.accessIndexId(),
                    statement.indexRange(), statement.predicates(),
                    statement.lockMode(), deadline).rows();
            // 4、scan helper 已完整通过双容量上限；投影阶段任一失败仍不会返回已构造前缀。
            return projectRows(records, statement.projectionOrdinals(), statement.table(),
                    mapped.clusteredIndex(), deadline);
        });
    }

    /**
     * 范围 UPDATE 先完成 FOR UPDATE scan 与 identity 物化，再在一个 statement guard 内逐点应用 patch。
     */
    @Override
    public SqlWriteOutcome updateRange(SqlTransactionHandle transaction, BoundRangeUpdate statement,
                                       SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("range UPDATE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound range UPDATE/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1、无匹配证明早于事务 id、锁和 undo；不会把 no-op 误记为写事务。
            if (statement.empty()) {
                return new SqlWriteOutcome(0, handle.transaction.rollbackOnly());
            }
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex clustered = mapped.clusteredIndex();
            // 2、完整 scan 在首笔 mutation 前取得/复核所有锁并通过容量上限，避免 partial DML 和 Halloween。
            LockedSelection selected = readLockingRange(handle, mapped, statement.accessIndexId(),
                    statement.indexRange(), statement.predicates(), SelectLockMode.FOR_UPDATE, deadline);
            ArrayList<TableColumnAssignment> assignments =
                    new ArrayList<>(statement.assignmentOrdinals().size());
            for (int i = 0; i < statement.assignmentOrdinals().size(); i++) {
                int ordinal = statement.assignmentOrdinals().get(i);
                assignments.add(new TableColumnAssignment(ordinal, toColumnValue(
                        statement.assignmentValues().get(i), clustered.schema().column(ordinal).type())));
            }
            // 3、一个 guard 覆盖所有 row mutation；任一行失败会把本语句此前写入全部反向应用。
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, clustered);
            int affected = 0;
            try {
                for (SearchKey identity : selected.identities()) {
                    DmlWriteResult result = engine.tableDmlService().update(new TableUpdatePatchCommand(
                            handle.transaction, mapped.tableIndexes(), identity, assignments,
                            mapped.lobSegment(), deadline.cap(operationTimeout,
                            "range UPDATE clustered row-lock wait")));
                    affected += result.affectedRows();
                }
                // 4、全部 point mutation 成功后才关闭 guard/发布 wrote；close 失败仍进入统一 rollback 分支。
                guard.close();
                handle.wrote |= affected > 0;
                return new SqlWriteOutcome(affected, handle.transaction.rollbackOnly());
            } catch (RuntimeException writeFailure) {
                rollbackRangeStatement(guard, writeFailure, handle, "range UPDATE");
                throw adapt("range UPDATE failed after confirmed statement rollback", writeFailure);
            }
        });
    }

    /**
     * 范围 DELETE 与 UPDATE 共享先物化 identity 的 Halloween/partial 防线，随后在同一 guard 内逐点标删。
     */
    @Override
    public SqlWriteOutcome deleteRange(SqlTransactionHandle transaction, BoundRangeDelete statement,
                                       SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("range DELETE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound range DELETE/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1、empty 不触发事务写身份。
            if (statement.empty()) {
                return new SqlWriteOutcome(0, handle.transaction.rollbackOnly());
            }
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex clustered = mapped.clusteredIndex();
            // 2、所有 candidate/record locks 与 residual 在写入前完成。
            LockedSelection selected = readLockingRange(handle, mapped, statement.accessIndexId(),
                    statement.indexRange(), statement.predicates(), SelectLockMode.FOR_UPDATE, deadline);
            // 3、单 guard 覆盖逐 identity delete-mark 及所有二级/LOB undo 副作用。
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, clustered);
            int affected = 0;
            try {
                for (SearchKey identity : selected.identities()) {
                    DmlWriteResult result = engine.tableDmlService().delete(new TableDeleteCommand(
                            handle.transaction, mapped.tableIndexes(), identity, mapped.lobSegment(),
                            deadline.cap(operationTimeout, "range DELETE clustered row-lock wait")));
                    affected += result.affectedRows();
                }
                // 4、全部成功后一次发布 statement terminal。
                guard.close();
                handle.wrote |= affected > 0;
                return new SqlWriteOutcome(affected, handle.transaction.rollbackOnly());
            } catch (RuntimeException writeFailure) {
                rollbackRangeStatement(guard, writeFailure, handle, "range DELETE");
                throw adapt("range DELETE failed after confirmed statement rollback", writeFailure);
            }
        });
    }

    /**
     * 一致性范围读：一个 ReadView 覆盖所有 256-row 批次与回表版本链；RC finally 注销。
     */
    private List<SqlRow> readConsistentRange(
            EngineSqlTransactionHandle handle, MappedTableStorage mapped,
            BoundRangeSelect statement, SqlStatementDeadline deadline) {
        if (handle.transaction.isolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
            List<LogicalRecord> records = scanReadUncommittedBatches(
                    mapped, statement.accessIndexId(), statement.indexRange(),
                    statement.predicates(), deadline);
            return projectRows(records, statement.projectionOrdinals(), statement.table(),
                    mapped.clusteredIndex(), deadline);
        }
        ReadViewManager views = engine.transactionManager().readViewManager();
        deadline.remaining("comparison range ReadView creation");
        ReadView view = views.openReadView(handle.transaction);
        RuntimeException failure = null;
        try {
            List<LogicalRecord> records = scanMvccBatches(
                    view, mapped, statement.accessIndexId(), statement.indexRange(),
                    statement.predicates(), deadline);
            // RC view 必须继续存活到所有 external LOB chain 解引用和公开值构造完成。
            return projectRows(records, statement.projectionOrdinals(), statement.table(),
                    mapped.clusteredIndex(), deadline);
        } catch (RuntimeException readFailure) {
            failure = adapt("comparison range MVCC scan failed", readFailure);
            throw failure;
        } finally {
            if (handle.transaction.isolationLevel() == IsolationLevel.READ_COMMITTED) {
                try {
                    views.closeReadView(view);
                } catch (RuntimeException closeFailure) {
                    RuntimeException adapted = adapt(
                            "close READ COMMITTED comparison range ReadView failed", closeFailure);
                    if (failure == null) {
                        throw adapted;
                    }
                    failure.addSuppressed(adapted);
                }
            }
        }
    }

    /**
     * 分页扫描 physical candidates，逐聚簇 identity 执行 MVCC 与 residual。
     */
    private List<LogicalRecord> scanMvccBatches(
            ReadView view, MappedTableStorage mapped, long accessIndexId,
            BoundIndexRange range, List<BoundRowPredicate> predicates,
            SqlStatementDeadline deadline) {
        BTreeIndex access = mapped.index(accessIndexId);
        BTreeIndex clustered = mapped.clusteredIndex();
        SecondaryIndexMetadata secondary = access.clustered()
                ? null : mapped.tableIndexes().requireSecondary(accessIndexId);
        ArrayList<SearchKey> identities = new ArrayList<>();
        ArrayList<LogicalRecord> rows = new ArrayList<>();
        SearchKey continuation = null;
        int physicalCandidates = 0;
        while (true) {
            // 1、每批建立独立只读 MTR；上一批完整 physical key 作为 exclusive continuation。
            deadline.remaining("comparison range physical batch");
            BTreeScanRange batchRange = toBTreeRange(
                    range, access, continuation, RANGE_SCAN_BATCH_SIZE);
            MiniTransaction mtr = engine.miniTransactionManager().beginReadOnly();
            List<BTreeLookupResult> batch;
            try {
                batch = access.clustered()
                        ? engine.btreeService().scanClusteredIncludingDeleted(mtr, access, batchRange)
                        : engine.btreeService().scanIncludingDeleted(mtr, access, batchRange);
                engine.miniTransactionManager().commit(mtr);
            } catch (RuntimeException scanFailure) {
                rollbackMtr(mtr, scanFailure);
                throw scanFailure;
            }
            if (batch.isEmpty()) {
                return List.copyOf(rows);
            }

            // 2、批次返回时所有 page latch/fix 已释放；后续 undo/回表不会与 access leaf 资源重叠。
            physicalCandidates += batch.size();
            requirePhysicalCapacity(physicalCandidates, mapped, access);
            for (BTreeLookupResult candidate : batch) {
                SearchKey identity = access.clustered()
                        ? keyFromRow(candidate.record().columnValues(), clustered)
                        : secondary.layout().clusterKey(candidate.record());
                if (containsIdentity(identities, identity, clustered)) {
                    continue;
                }
                Optional<LogicalRecord> visible = engine.mvccReader().read(view, clustered, identity);
                if (visible.isEmpty()) {
                    identities.add(identity);
                    continue;
                }
                LogicalRecord row = visible.orElseThrow();
                if (matchesPredicates(row, predicates, clustered)) {
                    identities.add(identity);
                    rows.add(row);
                    requireRowCapacity(rows.size(), mapped, access);
                } else {
                    identities.add(identity);
                }
            }
            // 3、完整 physical key 而非 logical prefix 推进，保证 non-unique suffix 不重复/遗漏。
            continuation = keyFromRow(batch.getLast().record().columnValues(), access);
            // 4、短批表示已越过上界/到达最右 leaf；满批继续一次以确认是否结束。
            if (batch.size() < RANGE_SCAN_BATCH_SIZE) {
                return List.copyOf(rows);
            }
        }
    }

    /**
     * RU 分页扫描：physical access entry 只作当前候选，每条都回聚簇读取最新未标删版本并重算 residual。
     * 不创建 ReadView、不遍历 undo，也不持 access leaf 资源进入聚簇读取。
     */
    private List<LogicalRecord> scanReadUncommittedBatches(
            MappedTableStorage mapped, long accessIndexId,
            BoundIndexRange range, List<BoundRowPredicate> predicates,
            SqlStatementDeadline deadline) {
        BTreeIndex access = mapped.index(accessIndexId);
        BTreeIndex clustered = mapped.clusteredIndex();
        SecondaryIndexMetadata secondary = access.clustered()
                ? null : mapped.tableIndexes().requireSecondary(accessIndexId);
        ArrayList<SearchKey> identities = new ArrayList<>();
        ArrayList<LogicalRecord> rows = new ArrayList<>();
        SearchKey continuation = null;
        int physicalCandidates = 0;
        while (true) {
            // 1、每批只在短只读 MTR 内物化 access candidate；including-deleted 让历史 entry 不会掩盖当前重算。
            deadline.remaining("read-uncommitted physical batch");
            BTreeScanRange batchRange = toBTreeRange(
                    range, access, continuation, RANGE_SCAN_BATCH_SIZE);
            MiniTransaction mtr = engine.miniTransactionManager().beginReadOnly();
            List<BTreeLookupResult> batch;
            try {
                batch = access.clustered()
                        ? engine.btreeService().scanClusteredIncludingDeleted(mtr, access, batchRange)
                        : engine.btreeService().scanIncludingDeleted(mtr, access, batchRange);
                engine.miniTransactionManager().commit(mtr);
            } catch (RuntimeException scanFailure) {
                rollbackMtr(mtr, scanFailure);
                throw scanFailure;
            }
            if (batch.isEmpty()) {
                return List.copyOf(rows);
            }

            // 2、access 页资源已释放；逐候选提取聚簇 identity 并按聚簇比较语义去重。
            physicalCandidates += batch.size();
            requirePhysicalCapacity(physicalCandidates, mapped, access);
            for (BTreeLookupResult candidate : batch) {
                SearchKey identity = access.clustered()
                        ? keyFromRow(candidate.record().columnValues(), clustered)
                        : secondary.layout().clusterKey(candidate.record());
                if (containsIdentity(identities, identity, clustered)) {
                    continue;
                }
                identities.add(identity);

                // 3、RU 只接受读取瞬间的聚簇当前未标删版本；旧 secondary entry 必须由完整行 residual 排除。
                Optional<LogicalRecord> current =
                        engine.mvccReader().readUncommitted(clustered, identity);
                if (current.isPresent()
                        && matchesPredicates(current.orElseThrow(), predicates, clustered)) {
                    rows.add(current.orElseThrow());
                    requireRowCapacity(rows.size(), mapped, access);
                }
            }

            // 4、continuation 使用完整 physical key，保证 secondary clustered suffix 不遗漏或重复。
            continuation = keyFromRow(batch.getLast().record().columnValues(), access);
            if (batch.size() < RANGE_SCAN_BATCH_SIZE) {
                return List.copyOf(rows);
            }
        }
    }

    /**
     * current range read：短 MTR 定位后等待 access record/gap lock，secondary 再锁聚簇记录并复核 residual。
     */
    private LockedSelection readLockingRange(
            EngineSqlTransactionHandle handle, MappedTableStorage mapped, long accessIndexId,
            BoundIndexRange range, List<BoundRowPredicate> predicates, SelectLockMode lockMode,
            SqlStatementDeadline deadline) {
        BTreeIndex access = mapped.index(accessIndexId);
        BTreeIndex clustered = mapped.clusteredIndex();
        SecondaryIndexMetadata secondary = access.clustered()
                ? null : mapped.tableIndexes().requireSecondary(accessIndexId);
        TransactionId owner = engine.transactionManager().assignWriteId(handle.transaction);
        BTreeCurrentReadMode mode = lockMode == SelectLockMode.FOR_SHARE
                ? BTreeCurrentReadMode.FOR_SHARE : BTreeCurrentReadMode.FOR_UPDATE;
        ArrayList<SearchKey> identities = new ArrayList<>();
        ArrayList<LogicalRecord> rows = new ArrayList<>();
        SearchKey continuation = null;
        int physicalCandidates = 0;
        while (true) {
            // 1、currentRead 内部完成“短定位→释放页资源→等锁→重定位”；deadline 每批只能消费剩余预算。
            BTreeScanRange batchRange = toBTreeRange(
                    range, access, continuation, RANGE_SCAN_BATCH_SIZE);
            BTreeCurrentReadRequest request = new BTreeCurrentReadRequest(
                    owner, handle.transaction.isolationLevel(),
                    deadline.cap(operationTimeout, "comparison locking range"),
                    RANGE_RELOCATION_RETRIES);
            List<BTreeLookupResult> batch = engine.btreeCurrentReadService()
                    .lockRange(access, batchRange, request, mode);
            if (batch.isEmpty()) {
                return new LockedSelection(rows, identities);
            }
            physicalCandidates += batch.size();
            requirePhysicalCapacity(physicalCandidates, mapped, access);

            // 2、二级 access lock 授予后，逐候选再取聚簇 record lock；两次等待之间不持 B+Tree page 资源。
            for (BTreeLookupResult candidate : batch) {
                SearchKey identity;
                LogicalRecord row;
                if (access.clustered()) {
                    identity = keyFromRow(candidate.record().columnValues(), clustered);
                    row = candidate.record();
                } else {
                    identity = secondary.layout().clusterKey(candidate.record());
                    if (containsIdentity(identities, identity, clustered)) {
                        continue;
                    }
                    BTreeCurrentReadRequest clusteredRequest = new BTreeCurrentReadRequest(
                            owner, handle.transaction.isolationLevel(),
                            deadline.cap(operationTimeout, "comparison clustered row lock"),
                            RANGE_RELOCATION_RETRIES);
                    Optional<BTreeLookupResult> locked = engine.btreeCurrentReadService()
                            .lockPoint(clustered, identity, clusteredRequest, mode);
                    if (locked.isEmpty()) {
                        identities.add(identity);
                        continue;
                    }
                    row = locked.orElseThrow().record();
                }
                if (containsIdentity(identities, identity, clustered)) {
                    continue;
                }
                identities.add(identity);
                // 3、当前完整聚簇行是 residual 权威；marked secondary history/并发换 key 不得产生假命中。
                if (matchesPredicates(row, predicates, clustered)) {
                    rows.add(row);
                    requireRowCapacity(rows.size(), mapped, access);
                }
            }
            // 4、先物化完整 identity 集再返回给 range DML；continuation 永远来自 access physical key。
            continuation = keyFromRow(batch.getLast().record().columnValues(), access);
            if (batch.size() < RANGE_SCAN_BATCH_SIZE) {
                return new LockedSelection(rows, identitiesForRows(rows, clustered));
            }
        }
    }

    /**
     * 注意 current scan 的 identities 列表还包含 residual miss，用于候选去重；对 DML 只返回匹配 rows 的 identity。
     */
    private static List<SearchKey> identitiesForRows(List<LogicalRecord> rows, BTreeIndex clustered) {
        return rows.stream().map(row -> keyFromRow(row.columnValues(), clustered)).toList();
    }

    private BTreeScanRange toBTreeRange(BoundIndexRange range, BTreeIndex access,
                                        SearchKey continuation, int limit) {
        Optional<SearchKey> lower = continuation == null
                ? range.lower().map(endpoint -> endpointKey(endpoint, access))
                : Optional.of(continuation);
        boolean lowerInclusive = continuation == null
                ? range.lower().map(BoundRangeEndpoint::inclusive).orElse(true) : false;
        Optional<SearchKey> upper = range.upper().map(endpoint -> endpointKey(endpoint, access));
        boolean upperInclusive = range.upper().map(BoundRangeEndpoint::inclusive).orElse(true);
        return BTreeScanRange.of(lower, lowerInclusive, upper, upperInclusive, limit);
    }

    private SearchKey endpointKey(BoundRangeEndpoint endpoint, BTreeIndex access) {
        return new SearchKey(toKeyValues(endpoint.keyValues(), access));
    }

    /** 把完整 logical equality key 表达为双侧闭合前缀范围。 */
    private static BoundIndexRange equalityRange(List<SqlValue> values) {
        BoundRangeEndpoint endpoint = new BoundRangeEndpoint(values, true);
        return new BoundIndexRange(Optional.of(endpoint), Optional.of(endpoint));
    }

    /**
     * 从 exact DD index key parts 构造当前行复核谓词。secondary physical suffix 不在 values 中，
     * 因而只重算 logical parts；这正是排除 marked old entry 和并发换 key 假命中的权威判断。
     */
    private static List<BoundRowPredicate> equalityPredicates(
            TableDefinition table, long indexId, List<SqlValue> values) {
        IndexDefinition index = table.indexes().stream()
                .filter(candidate -> candidate.id().value() == indexId)
                .findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "equality predicate index is missing from exact table version"));
        if (values.size() != index.keyParts().size()) {
            throw new DatabaseValidationException(
                    "equality predicate value count differs from logical index parts");
        }
        ArrayList<BoundRowPredicate> predicates = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            long columnId = index.keyParts().get(i).columnId();
            int ordinal = table.columns().stream()
                    .filter(column -> column.columnId() == columnId)
                    .mapToInt(column -> column.ordinal())
                    .findFirst()
                    .orElseThrow(() -> new DatabaseValidationException(
                            "index key part references missing DD column"));
            predicates.add(new BoundRowPredicate(
                    ordinal, BoundRowPredicateOperator.EQUAL, values.get(i)));
        }
        return List.copyOf(predicates);
    }

    /**
     * 逐 residual 使用 Record codec 的一列 ASC 比较定义；NULL comparison 返回 UNKNOWN，conjunction 因而不匹配。
     */
    private boolean matchesPredicates(LogicalRecord row, List<BoundRowPredicate> predicates,
                                      BTreeIndex clustered) {
        for (BoundRowPredicate predicate : predicates) {
            ColumnValue left = row.columnValues().get(predicate.columnOrdinal());
            if (left instanceof ColumnValue.NullValue
                    || predicate.value() instanceof SqlValue.NullValue) {
                return false;
            }
            ColumnValue right = toColumnValue(predicate.value(),
                    clustered.schema().column(predicate.columnOrdinal()).type());
            IndexKeyDef oneColumn = new IndexKeyDef(clustered.indexId(), List.of(
                    new KeyPartDef(new ColumnId(predicate.columnOrdinal()), KeyOrder.ASC, 0)));
            int comparison = predicateComparator.compare(
                    new SearchKey(List.of(left)), new SearchKey(List.of(right)),
                    oneColumn, clustered.schema());
            boolean matched = switch (predicate.operator()) {
                case EQUAL -> comparison == 0;
                case LESS_THAN -> comparison < 0;
                case LESS_THAN_OR_EQUAL -> comparison <= 0;
                case GREATER_THAN -> comparison > 0;
                case GREATER_THAN_OR_EQUAL -> comparison >= 0;
            };
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private boolean containsIdentity(List<SearchKey> identities, SearchKey candidate,
                                     BTreeIndex clustered) {
        return identities.stream().anyMatch(existing -> predicateComparator.compare(
                existing, candidate, clustered.keyDef(), clustered.schema()) == 0);
    }

    private static void requirePhysicalCapacity(int count, MappedTableStorage mapped, BTreeIndex access) {
        if (count > RANGE_PHYSICAL_CANDIDATE_LIMIT) {
            throw new SqlStorageException("range physical candidate limit exceeded: table="
                    + mapped.table().id().value() + " index=" + access.indexId()
                    + " limit=" + RANGE_PHYSICAL_CANDIDATE_LIMIT);
        }
    }

    private static void requireRowCapacity(int count, MappedTableStorage mapped, BTreeIndex access) {
        if (count > RANGE_ROW_LIMIT) {
            throw new SqlStorageException("range row identity limit exceeded: table="
                    + mapped.table().id().value() + " index=" + access.indexId()
                    + " limit=" + RANGE_ROW_LIMIT);
        }
    }

    private void rollbackRangeStatement(DmlStatementGuard guard, RuntimeException failure,
                                        EngineSqlTransactionHandle handle, String operation) {
        try {
            guard.rollback();
        } catch (RuntimeException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
            throw new SqlStatementRollbackException(
                    operation + " failed and statement rollback was not confirmed",
                    handle.transaction.rollbackOnly(), failure);
        }
    }

    /** current scan 的匹配完整行及其对应聚簇 identity。 */
    private record LockedSelection(List<LogicalRecord> rows, List<SearchKey> identities) {
        private LockedSelection {
            rows = List.copyOf(rows);
            identities = List.copyOf(identities);
        }
    }

    /** 首写事务走 DML durability/lock 收尾；未成功写入的事务直接完成内存生命周期且释放可能存在的行锁。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code commit} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request) {
        if (request == null) throw new DatabaseValidationException("SQL commit request must not be null");
        return withActive(transaction, null, handle -> {
            rejectRecoveryExportWrittenTransaction(handle, "commit");
            try {
                SqlCommitOutcome outcome;
                if (handle.wrote || handle.transaction.undoContext() != null) {
                    DmlCommitResult committed = engine.dmlService().commit(new DmlCommitCommand(handle.transaction,
                            durability(request.durabilityMode()), request.timeout()));
                    outcome = new SqlCommitOutcome(committed.transactionNo().value(), committed.durable(),
                            committed.releasedLockCount());
                } else {
                    engine.transactionManager().commit(handle.transaction);
                    int released = releaseLocks(handle.transaction);
                    outcome = new SqlCommitOutcome(handle.transaction.transactionNo().value(), true, released);
                }
                handle.state = EngineSqlTransactionHandle.State.COMMITTED;
                return outcome;
            } catch (RuntimeException commitFailure) {
                if (handle.transaction.state() == TransactionState.COMMITTED) {
                    handle.state = EngineSqlTransactionHandle.State.COMMITTED;
                    throw new SqlTransactionOutcomeException("commit reached terminal state but response/durability failed",
                            true, true, commitFailure);
                }
                throw adapt("commit failed before terminal state", commitFailure);
            }
        });
    }

    /** 有 undo 使用 DD resolver 做跨表 full rollback；无 undo 走轻量生命周期，终态后才释放锁。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code rollback} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    @Override
    public SqlRollbackOutcome rollback(SqlTransactionHandle transaction) {
        return withActive(transaction, null, handle -> {
            rejectRecoveryExportWrittenTransaction(handle, "rollback");
            try {
                SqlRollbackOutcome outcome;
                if (handle.transaction.undoContext() != null) {
                    DmlRollbackResult rolled = engine.dmlService().rollback(
                            new ResolvedDmlRollbackCommand(handle.transaction));
                    outcome = new SqlRollbackOutcome(rolled.rollbackSummary().undoRecordsApplied(),
                            rolled.releasedLockCount());
                } else {
                    engine.transactionManager().rollback(handle.transaction);
                    outcome = new SqlRollbackOutcome(0, releaseLocks(handle.transaction));
                }
                handle.state = EngineSqlTransactionHandle.State.ROLLED_BACK;
                return outcome;
            } catch (RuntimeException rollbackFailure) {
                if (handle.transaction.state() == TransactionState.ROLLED_BACK) {
                    handle.state = EngineSqlTransactionHandle.State.ROLLED_BACK;
                    throw new SqlTransactionOutcomeException("rollback reached terminal state but cleanup response failed",
                            true, true, rollbackFailure);
                }
                if (handle.transaction.state() == TransactionState.ROLLING_BACK) {
                    handle.state = EngineSqlTransactionHandle.State.FAILED;
                    throw new SqlTransactionOutcomeException("rollback outcome is uncertain and requires recovery",
                            false, true, rollbackFailure);
                }
                throw adapt("rollback failed before physical rollback boundary", rollbackFailure);
            }
        });
    }

    /** 在任何 mutation facade 调用前拒绝 FORCE 导出写意图。 */
    private void rejectRecoveryExportWrite(String operation) {
        if (recoveryExportReadOnly) {
            throw new RecoveryExportWriteRejectedException(
                    operation + " is rejected in recovery export read-only mode");
        }
    }

    /** 导出模式只允许终结从未写入且没有 undo context 的轻量事务。 */
    private void rejectRecoveryExportWrittenTransaction(EngineSqlTransactionHandle handle, String operation) {
        if (recoveryExportReadOnly && (handle.wrote || handle.transaction.undoContext() != null)) {
            throw new RecoveryExportWriteRejectedException(
                    operation + " rejected a written transaction in recovery export read-only mode");
        }
    }

    private <T> T withActive(SqlTransactionHandle candidate, SqlStatementDeadline deadline,
                             java.util.function.Function<EngineSqlTransactionHandle, T> action) {
        requireEngineOpen();
        EngineSqlTransactionHandle handle = requireOwned(candidate);
        boolean acquired;
        try {
            Duration wait = deadline == null ? operationTimeout
                    : deadline.cap(operationTimeout, "SQL transaction handle wait");
            acquired = handle.operationLock.tryLock(wait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SqlTransactionStateException("interrupted while waiting for SQL transaction handle", error);
        }
        if (!acquired) throw new SqlTransactionStateException("SQL transaction handle is busy");
        try {
            if (handle.state != EngineSqlTransactionHandle.State.ACTIVE) {
                throw new SqlTransactionStateException("SQL transaction handle is terminal: " + handle.state);
            }
            return action.apply(handle);
        } finally {
            handle.operationLock.unlock();
        }
    }

    /**
     * 有界取得 XA prepared handle，且只允许 PREPARED 或已经选择的相同决议进入。
     */
    private <T> T withXaState(SqlTransactionHandle candidate, boolean commit,
                              java.util.function.Function<EngineSqlTransactionHandle, T> action) {
        requireEngineOpen();
        EngineSqlTransactionHandle handle = requireOwned(candidate);
        boolean acquired;
        try {
            acquired = handle.operationLock.tryLock(operationTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SqlTransactionStateException(
                    "interrupted while waiting for XA transaction handle", error);
        }
        if (!acquired) {
            throw new SqlTransactionStateException("XA transaction handle is busy");
        }
        try {
            EngineSqlTransactionHandle.State decided = commit
                    ? EngineSqlTransactionHandle.State.COMMIT_DECIDED
                    : EngineSqlTransactionHandle.State.ROLLBACK_DECIDED;
            if (handle.state != EngineSqlTransactionHandle.State.PREPARED
                    && handle.state != decided) {
                throw new SqlTransactionStateException(
                        "XA handle does not permit this phase-two decision: " + handle.state);
            }
            return action.apply(handle);
        } finally {
            handle.operationLock.unlock();
        }
    }

    private static void requirePositiveXaTimeout(Duration timeout, String operation) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "XA " + operation + " timeout must be positive");
        }
    }

    private EngineSqlTransactionHandle requireOwned(SqlTransactionHandle candidate) {
        if (!(candidate instanceof EngineSqlTransactionHandle handle) || handle.owner != this) {
            throw new SqlTransactionStateException("SQL transaction handle belongs to another gateway/implementation");
        }
        return handle;
    }

    private void requireEngineOpen() {
        if (engine.state() != EngineState.OPEN || engine.recoveryState() != RecoveryState.OPEN) {
            throw new SqlStorageException("storage recovery gate is not OPEN: engine=" + engine.state()
                    + ", recovery=" + engine.recoveryState());
        }
    }

    private List<ColumnValue> toColumnValues(List<SqlValue> sqlValues, BTreeIndex index) {
        if (sqlValues.size() != index.schema().columnCount()) {
            throw new SqlStorageException("bound row width differs from mapped storage schema");
        }
        ArrayList<ColumnValue> result = new ArrayList<>(sqlValues.size());
        for (int i = 0; i < sqlValues.size(); i++) {
            result.add(toColumnValue(sqlValues.get(i), index.schema().column(i).type()));
        }
        return List.copyOf(result);
    }

    private List<ColumnValue> toKeyValues(List<SqlValue> keys, BTreeIndex index) {
        if (keys.isEmpty() || keys.size() > index.keyDef().parts().size()) {
            throw new SqlStorageException("bound key width exceeds mapped access index key");
        }
        ArrayList<ColumnValue> result = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            int ordinal = index.keyDef().parts().get(i).columnId().value();
            result.add(toColumnValue(keys.get(i), index.schema().column(ordinal).type()));
        }
        return List.copyOf(result);
    }

    private static SearchKey keyFromRow(List<ColumnValue> values, BTreeIndex index) {
        return new SearchKey(index.keyDef().parts().stream()
                .map(part -> values.get(part.columnId().value())).toList());
    }

    private List<ColumnValue> hydrateExternalValues(List<ColumnValue> source, BTreeIndex index,
                                                    SqlStatementDeadline deadline) {
        ArrayList<ColumnValue> result = new ArrayList<>(source);
        for (int ordinal = 0; ordinal < result.size(); ordinal++) {
            if (!(result.get(ordinal) instanceof ColumnValue.ExternalValue external)) continue;
            deadline.remaining("external LOB hydration for column " + ordinal);
            MiniTransaction mtr = engine.miniTransactionManager().beginReadOnly();
            try {
                ColumnType type = index.schema().column(ordinal).type();
                ColumnValue value = engine.lobStorage().read(mtr, type, external);
                engine.miniTransactionManager().commit(mtr);
                deadline.remaining("external LOB hydration completion for column " + ordinal);
                result.set(ordinal, value);
            } catch (RuntimeException hydrateFailure) {
                rollbackMtr(mtr, hydrateFailure);
                throw adapt("external LOB hydration failed for column " + ordinal, hydrateFailure);
            }
        }
        return List.copyOf(result);
    }

    /**
     * 对完整 storage rows 执行 LOB hydration 与 exact-DD projection；任一失败时不返回 partial row list。
     *
     * @param records            已由 MVCC/current-read 选出的完整聚簇行。
     * @param projectionOrdinals Binder 验证的公开列顺序。
     * @param table              exact DD table version，提供 SQL 类型解释。
     * @param clusteredIndex     与 records schema 匹配的聚簇 descriptor。
     * @param deadline           每个 LOB chain 继续消费的 statement absolute deadline。
     * @return 完整不可变 SqlRow 列表。
     */
    private List<SqlRow> projectRows(List<LogicalRecord> records, List<Integer> projectionOrdinals,
                                     cn.zhangyis.db.dd.domain.TableDefinition table,
                                     BTreeIndex clusteredIndex, SqlStatementDeadline deadline) {
        ArrayList<SqlRow> rows = new ArrayList<>(records.size());
        for (LogicalRecord record : records) {
            List<ColumnValue> hydrated = hydrateExternalValues(
                    record.columnValues(), clusteredIndex, deadline);
            ArrayList<SqlValue> projected = new ArrayList<>(projectionOrdinals.size());
            for (int ordinal : projectionOrdinals) {
                projected.add(toSqlValue(hydrated.get(ordinal), table.columns().get(ordinal).type()));
            }
            rows.add(new SqlRow(projected));
        }
        return List.copyOf(rows);
    }

    /**
     * 校验当前状态后推进SQL 与存储引擎适配层状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void rollbackMtr(MiniTransaction mtr, RuntimeException original) {
        if (mtr.state() != MiniTransactionState.ACTIVE) return;
        try { engine.miniTransactionManager().rollbackUncommitted(mtr); }
        catch (RuntimeException rollbackFailure) { original.addSuppressed(rollbackFailure); }
    }

    private static ColumnValue toColumnValue(SqlValue value, ColumnType type) {
        if (value instanceof SqlValue.NullValue) return ColumnValue.NullValue.INSTANCE;
        if (value instanceof SqlValue.IntegerValue integer) return new ColumnValue.IntValue(integerBits(integer.value(), type));
        if (value instanceof SqlValue.FloatingValue floating) return new ColumnValue.DoubleValue(floating.value());
        if (value instanceof SqlValue.DecimalValue decimal) return new ColumnValue.DecimalValue(decimal.value());
        if (value instanceof SqlValue.StringValue string) return new ColumnValue.StringValue(string.value());
        if (value instanceof SqlValue.BytesValue bytes) return new ColumnValue.BinaryValue(bytes.value());
        if (value instanceof SqlValue.TemporalValue temporal) return new ColumnValue.TemporalValue(
                cn.zhangyis.db.storage.record.type.TemporalKind.valueOf(temporal.kind().name()), temporal.value());
        if (value instanceof SqlValue.BitValue bit) {
            if (bit.bitWidth() != type.length()) throw new SqlStorageException("SQL BIT width differs from DD/storage type");
            return new ColumnValue.BitValue(bit.bytes());
        }
        if (value instanceof SqlValue.EnumValue enumeration) return new ColumnValue.EnumValue(enumeration.ordinal());
        if (value instanceof SqlValue.SetValue set) return new ColumnValue.SetValue(set.bitmap());
        throw new SqlStorageException("unsupported SQL value variant: " + value.getClass().getName());
    }

    private static long integerBits(BigInteger value, ColumnType type) {
        if (type.typeId() != cn.zhangyis.db.storage.record.schema.TypeId.TINYINT
                && type.typeId() != cn.zhangyis.db.storage.record.schema.TypeId.SMALLINT
                && type.typeId() != cn.zhangyis.db.storage.record.schema.TypeId.INT
                && type.typeId() != cn.zhangyis.db.storage.record.schema.TypeId.BIGINT) {
            throw new SqlStorageException("SQL integer supplied for non-integer storage type " + type.typeId());
        }
        int bits = switch (type.typeId()) {
            case TINYINT -> 8; case SMALLINT -> 16; case INT -> 32; case BIGINT -> 64;
            default -> throw new SqlStorageException("unreachable integer type mapping");
        };
        BigInteger min = type.unsigned() ? BigInteger.ZERO : TWO.pow(bits - 1).negate();
        BigInteger max = type.unsigned() ? TWO.pow(bits).subtract(BigInteger.ONE) : TWO.pow(bits - 1).subtract(BigInteger.ONE);
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
            throw new SqlStorageException("SQL integer is outside mapped storage range");
        }
        return value.longValue();
    }

    private static SqlValue toSqlValue(ColumnValue value, ColumnTypeDefinition type) {
        if (value instanceof ColumnValue.NullValue) return SqlValue.NullValue.INSTANCE;
        if (value instanceof ColumnValue.IntValue integer) {
            BigInteger projected = type.unsigned()
                    ? new BigInteger(Long.toUnsignedString(integer.value())) : BigInteger.valueOf(integer.value());
            return new SqlValue.IntegerValue(projected);
        }
        if (value instanceof ColumnValue.DoubleValue floating) return new SqlValue.FloatingValue(floating.value());
        if (value instanceof ColumnValue.DecimalValue decimal) return new SqlValue.DecimalValue(decimal.value());
        if (value instanceof ColumnValue.StringValue string) return new SqlValue.StringValue(string.value());
        if (value instanceof ColumnValue.BinaryValue bytes) return new SqlValue.BytesValue(bytes.value());
        if (value instanceof ColumnValue.TemporalValue temporal) return new SqlValue.TemporalValue(
                SqlValue.TemporalKind.valueOf(temporal.kind().name()), temporal.normalized());
        if (value instanceof ColumnValue.BitValue bit) return new SqlValue.BitValue(bit.value(), type.length());
        if (value instanceof ColumnValue.EnumValue enumeration) {
            int index = enumeration.ordinal() - 1;
            if (index < 0 || index >= type.symbols().size()) throw new SqlStorageException("stored ENUM ordinal is invalid");
            return new SqlValue.EnumValue(type.symbols().get(index), enumeration.ordinal());
        }
        if (value instanceof ColumnValue.SetValue set) {
            ArrayList<String> symbols = new ArrayList<>();
            for (int i = 0; i < type.symbols().size(); i++) if ((set.bitmap() & (1L << i)) != 0) symbols.add(type.symbols().get(i));
            return new SqlValue.SetValue(symbols, set.bitmap());
        }
        if (value instanceof ColumnValue.ExternalValue) throw new SqlStorageException("external value escaped LOB hydration");
        throw new SqlStorageException("unsupported stored value variant: " + value.getClass().getName());
    }

    private int releaseLocks(Transaction transaction) {
        return transaction.transactionId().isNone() ? 0 : engine.lockManager().releaseAll(transaction.transactionId());
    }

    /**
     * 校验保存点由当前 adapter 为同一事务创建且仍处于 OPEN；失败早于 undo/锁状态修改。
     */
    private EngineSqlSavepointHandle requireSavepoint(
            EngineSqlTransactionHandle transaction, SqlSavepointHandle savepoint) {
        if (!(savepoint instanceof EngineSqlSavepointHandle boundary)
                || boundary.owner != this
                || boundary.transaction != transaction.transaction
                || !boundary.open) {
            throw new DatabaseValidationException(
                    "SQL savepoint does not belong to the active transaction or is already closed");
        }
        return boundary;
    }

    private static DurabilityPolicy durability(SqlDurabilityMode mode) {
        return switch (mode) {
            case FLUSH_ON_COMMIT -> DurabilityPolicy.FLUSH_ON_COMMIT;
            case WRITE_ON_COMMIT -> DurabilityPolicy.WRITE_ON_COMMIT;
            case BACKGROUND_FLUSH -> DurabilityPolicy.BACKGROUND_FLUSH;
        };
    }

    private static RuntimeException adapt(String message, RuntimeException error) {
        DatabaseFatalException fatal = DatabaseFailureClassifier.preserveFatal(message, error);
        if (fatal != null) return fatal;
        return error instanceof SqlStorageException storage ? storage : new SqlStorageException(message, error);
    }
}
