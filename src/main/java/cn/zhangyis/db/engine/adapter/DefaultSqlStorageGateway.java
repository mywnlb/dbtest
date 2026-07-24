package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseFailureClassifier;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.executor.expression.ExpressionEvaluator;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;
import cn.zhangyis.db.sql.type.InsertValueSource;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.*;
import cn.zhangyis.db.sql.optimizer.physical.*;
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
import cn.zhangyis.db.common.exception.RecoveryExportWriteRejectedException;
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
import cn.zhangyis.db.storage.record.type.BinaryCollation;
import cn.zhangyis.db.storage.record.type.CollationStrategy;
import cn.zhangyis.db.storage.record.type.LobCodec;
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
    /** residual comparison 共享的 Record type/collation registry。 */
    private final TypeCodecRegistry predicateTypeCodecs =
            new TypeCodecRegistry();
    /** 普通 inline 类型复用 Record 的 NULL/type/collation 排序，不在 SQL adapter 复制比较规则。 */
    private final SearchKeyComparator predicateComparator =
            new SearchKeyComparator(predicateTypeCodecs);
    /** SELECT Filter 与 range DML 共用的 canonical 三值解释实现；本对象无共享可变状态。 */
    private final ExpressionEvaluator expressionEvaluator =
            new ExpressionEvaluator();

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
     *     <li>先用自增占位值验证全部行的 record shape，常量错误发生在 high-water 消耗之前。</li>
     *     <li>按输入顺序持久分配自增值并物化全部 record；页 0 durable 等待不持 statement guard。</li>
     *     <li>创建一个 statement guard 后顺序调用 DML facade；任一行失败由同一 guard 回滚全部已写行。</li>
     *     <li>成功关闭 guard并发布总 affected rows/第一生成键；失败保留 rollback-only/fatal 语义。</li>
     * </ol>
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param deadline SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @return {@code insert} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public SqlWriteOutcome insert(SqlTransactionHandle transaction, PhysicalInsert statement,
                                  SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("INSERT");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("bound INSERT/deadline must not be null");
        }
        return withActive(transaction, deadline, handle -> {
            // 1. mapper 只消费 binder 固定的 DD version，不重新按表名读取可能已变化的 metadata。
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex index = mapped.clusteredIndex();
            // 2. 所有常量/行宽先转换；自增占位值只用于无副作用 record codec 校验。
            validateInsertBatch(statement, index);
            // 3. 页0 high-water 在普通 DML guard 前持久化；rollback 有意不回收已发值。
            ResolvedInsertBatch resolved = resolveInsertBatch(
                    statement, mapped, deadline);
            List<LogicalRecord> records = resolved.rows().stream().map(row ->
                    new LogicalRecord(
                            index.schema().schemaVersion(),
                            toColumnValues(row, index), false,
                            RecordType.CONVENTIONAL)).toList();
            // 4. 一个 guard 覆盖整批物理写入；中途失败由 partial rollback 恢复 statement 起点。
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, index);
            try {
                long affectedRows = 0;
                boolean changed = false;
                for (LogicalRecord record : records) {
                    DmlWriteResult result = engine.tableDmlService().insert(
                            new TableInsertCommand(
                                    handle.transaction,
                                    mapped.tableIndexes(), record,
                                    mapped.lobSegment(),
                                    deadline.cap(
                                            operationTimeout,
                                            "clustered INSERT row-lock wait")));
                    affectedRows = Math.addExact(
                            affectedRows, result.affectedRows());
                    changed |= result.changed();
                }
                // 5. guard.close 成功才允许 handle 记为已写；失败路径由 catch 汇总 partial rollback 结果。
                guard.close();
                handle.wrote |= changed;
                return new SqlWriteOutcome(
                        affectedRows, handle.transaction.rollbackOnly(),
                        resolved.firstGeneratedKey());
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

    /**
     * 在自增分配前验证整批行可映射到 Record schema。生成单元格使用合法正占位值，验证结果不发布。
     */
    private void validateInsertBatch(
            PhysicalInsert statement, BTreeIndex index) {
        for (List<InsertValueSource> sources : statement.batch().rows()) {
            List<SqlValue> values = sources.stream().map(source ->
                    source instanceof InsertValueSource.Constant constant
                            ? constant.value()
                            : new SqlValue.IntegerValue(BigInteger.ONE))
                    .toList();
            toColumnValues(values, index);
        }
    }

    /**
     * 把 value sources 解析为普通 typed rows。显式自增正值与生成请求在一次 storage 调用中按行顺序推进
     * high-water；无自增表禁止出现生成来源。
     */
    private ResolvedInsertBatch resolveInsertBatch(
            PhysicalInsert statement, MappedTableStorage mapped,
            SqlStatementDeadline deadline) {
        int autoOrdinal = -1;
        for (var column : statement.table().columns()) {
            if (column.generation()
                    == cn.zhangyis.db.dd.domain.ColumnGeneration.AUTO_INCREMENT) {
                autoOrdinal = column.ordinal();
                break;
            }
        }
        boolean containsGenerated = statement.batch().rows().stream()
                .flatMap(List::stream)
                .anyMatch(InsertValueSource.AutoIncrement.class::isInstance);
        if (autoOrdinal < 0) {
            if (containsGenerated) {
                throw new DatabaseValidationException(
                        "INSERT generation source requires AUTO_INCREMENT metadata");
            }
            return new ResolvedInsertBatch(
                    statement.batch().rows().stream()
                            .map(DefaultSqlStorageGateway::constantRow)
                            .toList(),
                    Optional.empty());
        }

        List<Optional<BigInteger>> requests = new ArrayList<>(
                statement.batch().rows().size());
        for (List<InsertValueSource> row : statement.batch().rows()) {
            InsertValueSource source = row.get(autoOrdinal);
            if (source instanceof InsertValueSource.AutoIncrement) {
                requests.add(Optional.empty());
            } else {
                SqlValue value =
                        ((InsertValueSource.Constant) source).value();
                if (!(value instanceof SqlValue.IntegerValue integer)) {
                    throw new DatabaseValidationException(
                            "AUTO_INCREMENT source must be an integer");
                }
                requests.add(Optional.of(integer.value()));
            }
        }
        ColumnTypeDefinition autoType =
                statement.table().columns().get(autoOrdinal).type();
        var allocation = engine.autoIncrementService().allocate(
                 mapped.binding().spaceId(), requests,
                 autoIncrementMaximum(autoType),
                 deadline.remaining("AUTO_INCREMENT durable allocation"));
        List<List<SqlValue>> rows = new ArrayList<>(
                statement.batch().rows().size());
        for (int rowIndex = 0;
             rowIndex < statement.batch().rows().size(); rowIndex++) {
            List<SqlValue> values =
                    new ArrayList<>(constantRowWithGeneratedPlaceholder(
                            statement.batch().rows().get(rowIndex)));
            values.set(autoOrdinal, new SqlValue.IntegerValue(
                    allocation.values().get(rowIndex)));
            rows.add(List.copyOf(values));
        }
        return new ResolvedInsertBatch(
                 rows, allocation.firstGeneratedKey());
    }

    /**
     * 将 DD 整数类型转换为当前 AUTO_INCREMENT 列可生成的最大正值。
     *
     * <p>该边界必须在页 0 high-water 更新前参与分配，不能等到 Record 编码时再拒绝，否则一次失败
     * INSERT 会永久把发号器推进到列域之外。列类型已由 CREATE/binder 保证为整数，本方法仍 fail-closed
     * 拒绝非整数元数据，防止损坏 DD 绕过该不变量。</p>
     *
     * @param type AUTO_INCREMENT 列的 exact DD 类型；必须是 TINYINT、SMALLINT、INT 或 BIGINT
     * @return 按整数位宽和 signed/unsigned 属性计算的最大正值；结果可由页 0 无符号 64 位字段表达
     * @throws DatabaseValidationException 元数据不是 AUTO_INCREMENT 可用的整数类型时抛出；调用方不得发号
     */
    private static BigInteger autoIncrementMaximum(
            ColumnTypeDefinition type) {
        int bits = switch (type.typeId()) {
            case TINYINT -> Byte.SIZE;
            case SMALLINT -> Short.SIZE;
            case INT -> Integer.SIZE;
            case BIGINT -> Long.SIZE;
            default -> throw new DatabaseValidationException(
                    "AUTO_INCREMENT metadata requires an integer column");
        };
        int exponent = type.unsigned() ? bits : bits - 1;
        return TWO.pow(exponent).subtract(BigInteger.ONE);
    }

    private static List<SqlValue> constantRow(
            List<InsertValueSource> sources) {
        if (sources.stream().anyMatch(
                InsertValueSource.AutoIncrement.class::isInstance)) {
            throw new DatabaseValidationException(
                    "unresolved AUTO_INCREMENT source in constant row");
        }
        return sources.stream()
                .map(source -> ((InsertValueSource.Constant) source).value())
                .toList();
    }

    private static List<SqlValue> constantRowWithGeneratedPlaceholder(
            List<InsertValueSource> sources) {
        return sources.stream().map(source ->
                source instanceof InsertValueSource.Constant constant
                        ? constant.value()
                        : new SqlValue.IntegerValue(BigInteger.ONE))
                .toList();
    }

    /** 已解析批次只属于当前 statement，不跨线程或缓存发布。 */
    private record ResolvedInsertBatch(
            List<List<SqlValue>> rows,
            Optional<BigInteger> firstGeneratedKey) {
        private ResolvedInsertBatch {
            if (rows == null || rows.isEmpty()
                    || firstGeneratedKey == null) {
                throw new DatabaseValidationException(
                        "resolved INSERT batch is invalid");
            }
            rows = rows.stream().map(List::copyOf).toList();
        }
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
    public SqlWriteOutcome update(SqlTransactionHandle transaction, PhysicalPointUpdate statement,
                                  SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("UPDATE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("physical point UPDATE/deadline must not be null");
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
    public SqlWriteOutcome delete(SqlTransactionHandle transaction, PhysicalPointDelete statement,
                                  SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("DELETE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException("physical point DELETE/deadline must not be null");
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
     * 打开兼容单 cursor，并通过内部 statement scope 持有 transaction operation lease。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 access/deadline，并创建一个正式 statement cursor scope。</li>
     *     <li>在 scope 内按 exact DD version 映射并打开唯一 child cursor。</li>
     *     <li>用包装器把旧 API 的 cursor close 转换为 child close 后 scope close。</li>
     *     <li>任一构造失败关闭 scope，确保 RC ReadView 与 operation lease 不泄漏。</li>
     * </ol>
     *
     * @param transaction 本 adapter 创建且仍 ACTIVE 的不透明事务能力
     * @param access Optimizer 产生且不包含 residual/projection 的访问叶
     * @param deadline parse/bind/optimize/execute 共用的绝对期限
     * @return 已打开且必须关闭的 pull cursor
     * @throws DatabaseValidationException access/deadline 缺失时抛出
     * @throws SqlTransactionStateException handle 忙、跨 adapter 或已终态时抛出
     * @throws SqlStorageException metadata/ReadView/cursor 初始化失败时抛出
     */
    @Override
    public SqlStorageCursor openCursor(
            SqlTransactionHandle transaction, PhysicalAccess access,
            SqlStatementDeadline deadline) {
        // 单 cursor 兼容入口也复用正式 scope；返回包装器在 cursor close 后关闭 scope。
        if (access == null || deadline == null) {
            throw new DatabaseValidationException(
                    "physical access/deadline must not be null");
        }
        SqlCursorScope scope =
                openCursorScope(transaction, deadline);
        try {
            return new ScopedSqlStorageCursor(
                    scope.openCursor(access), scope);
        } catch (RuntimeException openFailure) {
            try {
                scope.close();
            } catch (RuntimeException closeFailure) {
                openFailure.addSuppressed(closeFailure);
            }
            throw openFailure;
        }
    }

    /**
     * 打开一个可拥有多个 child cursor 的查询语句 scope。
     *
     * <ol>
     *     <li>校验 deadline 与 engine gate，并有界取得 transaction operation lock。</li>
     *     <li>锁内确认 handle ACTIVE 且没有其它 cursor scope；成功发布 cursorActive。</li>
     *     <li>返回单 owner scope；后续 cursor 映射、ReadView 延迟创建与关闭均不再次获取 handle lock。</li>
     *     <li>构造失败在本栈帧释放 lock；成功后只有 scope close 释放，阻止 statement 中途 commit/rollback。</li>
     * </ol>
     *
     * @param transaction 本 adapter 创建的 ACTIVE transaction handle
     * @param deadline parse/bind/optimize/execute 共用的绝对期限
     * @return 可同时持有 outer/inner cursor 的语句资源作用域
     */
    @Override
    public SqlCursorScope openCursorScope(
            SqlTransactionHandle transaction,
            SqlStatementDeadline deadline) {
        // 1、scope 是事务并发边界，输入错误必须早于 lock/read-view 副作用。
        if (deadline == null) {
            throw new DatabaseValidationException(
                    "SQL cursor scope deadline must not be null");
        }
        requireEngineOpen();
        EngineSqlTransactionHandle handle =
                requireOwned(transaction);
        acquireCursorLease(handle, deadline);
        boolean transferred = false;
        try {
            // 2、ReentrantLock 同线程可重入，因此 cursorActive 是不可省略的语义防线。
            if (handle.state
                    != EngineSqlTransactionHandle.State.ACTIVE) {
                throw new SqlTransactionStateException(
                        "SQL transaction handle is terminal: "
                                + handle.state);
            }
            if (handle.cursorActive) {
                throw new SqlTransactionStateException(
                        "SQL transaction handle already owns an active cursor scope");
            }
            handle.cursorActive = true;
            // 3、ReadView 延迟到首个非 empty consistent cursor，LIMIT 0 不创建历史快照。
            EngineSqlCursorScope scope =
                    new EngineSqlCursorScope(
                            handle, deadline);
            transferred = true;
            return scope;
        } catch (RuntimeException openFailure) {
            throw adapt(
                    "open SQL cursor scope failed",
                    openFailure);
        } finally {
            // 4、只有完整发布的 scope 接管 operation lease。
            if (!transferred) {
                handle.operationLock.unlock();
            }
        }
    }

    /**
     * 使用语句剩余时间取得 cursor-scope 独占 lease；该 lease 跨全部 child advance，
     * 只能由 scope close 释放。
     */
    private void acquireCursorLease(
            EngineSqlTransactionHandle handle,
            SqlStatementDeadline deadline) {
        boolean acquired;
        try {
            Duration wait = deadline.cap(
                    operationTimeout, "SQL cursor transaction handle wait");
            acquired = handle.operationLock.tryLock(
                    wait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new SqlTransactionStateException(
                    "interrupted while waiting for SQL cursor transaction handle",
                    error);
        }
        if (!acquired) {
            throw new SqlTransactionStateException(
                    "SQL transaction handle is busy");
        }
    }

    /**
     * 生产 cursor scope：共享一个 operation lease、deadline 与一致性 ReadView。
     * 该对象只由执行 statement 的线程使用，不以全局锁串行化其它事务。
     */
    private final class EngineSqlCursorScope
            implements SqlCursorScope {
        /** scope 生命周期内独占的 opaque transaction handle。 */
        private final EngineSqlTransactionHandle handle;
        /** 所有 child cursor 共享的绝对 statement deadline。 */
        private final SqlStatementDeadline deadline;
        /** 已创建 cursor 的稳定顺序；close 按反序幂等收口。 */
        private final ArrayList<AbstractEngineSqlStorageCursor> cursors =
                new ArrayList<>();
        /** 首个 consistent cursor 延迟创建的 statement ReadView。 */
        private ReadView view;
        /** RC view 的注销 owner；RU/尚未创建时为空。 */
        private ReadViewManager views;
        /** close 的单 owner 权威状态。 */
        private boolean closed;

        private EngineSqlCursorScope(
                EngineSqlTransactionHandle handle,
                SqlStatementDeadline deadline) {
            this.handle = handle;
            this.deadline = deadline;
        }

        /**
         * 创建 scope-owned cursor；多个 cursor 复用同一 ReadView，但各自保留独立扫描状态。
         */
        @Override
        public SqlStorageCursor openCursor(
                PhysicalAccess access) {
            if (closed) {
                throw new SqlStorageException(
                        "SQL cursor scope is closed");
            }
            if (access == null) {
                throw new DatabaseValidationException(
                        "physical access must not be null");
            }
            try {
                MappedTableStorage mapped =
                        mapper.map(access.table());
                AbstractEngineSqlStorageCursor cursor =
                        switch (access) {
                            case PhysicalPointAccess point ->
                                    new PointSqlStorageCursor(
                                            this, mapped, point,
                                            deadline);
                            case PhysicalSecondaryPrefixAccess secondary ->
                                    new SecondaryPrefixSqlStorageCursor(
                                            this, mapped, secondary,
                                            deadline);
                            case PhysicalRangeAccess range ->
                                    new RangeSqlStorageCursor(
                                            this, mapped, range,
                                            deadline);
                        };
                cursor.initializeReadView();
                cursors.add(cursor);
                return cursor;
            } catch (RuntimeException openFailure) {
                throw adapt(
                        "open SQL storage cursor in statement scope failed",
                        openFailure);
            }
        }

        /**
         * 为 consistent cursor 返回 scope 唯一 ReadView；empty/RU/locking 返回 null。
         */
        private ReadView readView(
                boolean consistent, boolean empty) {
            if (empty || !consistent
                    || handle.transaction.isolationLevel()
                    == IsolationLevel.READ_UNCOMMITTED) {
                return null;
            }
            if (view == null) {
                deadline.remaining(
                        "SQL cursor scope ReadView creation");
                views = engine.transactionManager()
                        .readViewManager();
                view = views.openReadView(
                        handle.transaction);
            }
            return view;
        }

        /**
         * 反序关闭全部 cursor，再注销 RC view 并最终释放 operation lease。
         */
        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            RuntimeException failure = null;
            for (int index = cursors.size() - 1;
                 index >= 0; index--) {
                try {
                    cursors.get(index).close();
                } catch (RuntimeException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(
                                closeFailure);
                    }
                }
            }
            cursors.clear();
            if (views != null
                    && handle.transaction.isolationLevel()
                    == IsolationLevel.READ_COMMITTED) {
                try {
                    views.closeReadView(view);
                } catch (RuntimeException closeFailure) {
                    RuntimeException adapted = adapt(
                            "close SQL cursor scope READ COMMITTED ReadView failed",
                            closeFailure);
                    if (failure == null) {
                        failure = adapted;
                    } else {
                        failure.addSuppressed(
                                adapted);
                    }
                }
            }
            try {
                handle.cursorActive = false;
                handle.operationLock.unlock();
            } catch (RuntimeException unlockFailure) {
                RuntimeException adapted = adapt(
                        "release SQL cursor scope transaction lease failed",
                        unlockFailure);
                if (failure == null) {
                    failure = adapted;
                } else {
                    failure.addSuppressed(
                            adapted);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /**
     * 旧单 cursor API 的所有权适配器；close 必须先使 row view 失效，再关闭整个 scope。
     */
    private static final class ScopedSqlStorageCursor
            implements SqlStorageCursor {
        private final SqlStorageCursor cursor;
        private final SqlCursorScope scope;
        private boolean closed;

        private ScopedSqlStorageCursor(
                SqlStorageCursor cursor,
                SqlCursorScope scope) {
            this.cursor = cursor;
            this.scope = scope;
        }

        @Override
        public boolean advance() {
            return cursor.advance();
        }

        @Override
        public SqlRowView current() {
            return cursor.current();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            RuntimeException failure = null;
            try {
                cursor.close();
            } catch (RuntimeException closeFailure) {
                failure = closeFailure;
            }
            try {
                scope.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(
                            closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /**
     * adapter cursor 公共状态机。current row 只保存已脱离 page latch 的 LogicalRecord；
     * generation 使旧 SqlRowView 在下一次 advance/close 后稳定失败。
     */
    private abstract class AbstractEngineSqlStorageCursor
            implements SqlStorageCursor {
        /** operation lease/ReadView 的 statement owner。 */
        private final EngineSqlCursorScope scope;
        /** statement scope 统一持有 operation lease，本 cursor 只借用事务身份。 */
        private final EngineSqlTransactionHandle handle;
        /** exact DD version 派生的全部 table/index mapping。 */
        protected final MappedTableStorage mapped;
        /** projection/hydration/scan 共用的绝对语句期限。 */
        protected final SqlStatementDeadline deadline;
        /** 完整逻辑行、LOB 与 exact comparator 共同使用的聚簇 descriptor。 */
        protected final BTreeIndex clustered;
        /** 是否为非 locking consistent read；决定初始化阶段是否创建 view。 */
        private final boolean consistent;
        /** optimizer empty 证明；该 cursor 不得创建 view 或访问存储。 */
        private final boolean empty;
        /** consistent read 的 view；初始化前以及 RU/locking/empty cursor 为 null。 */
        protected ReadView view;
        /** 当前 row-view generation；advance/close 单调增加。 */
        private long generation;
        /** 最近一次成功 advance 的完整逻辑行。 */
        private LogicalRecord current;
        /** close 的唯一权威状态；由 statement 线程单 owner 更新。 */
        private boolean closed;

        /**
         * 创建 cursor 并按读意图决定是否打开 ReadView。
         *
         * @param handle 已持有 operation lock 的 ACTIVE handle
         * @param mapped exact table mapping
         * @param deadline statement absolute deadline
         * @param consistent 是否为非 locking consistent read
         * @param empty 是否为 optimizer empty 证明；empty 不创建 view
         */
        protected AbstractEngineSqlStorageCursor(
                EngineSqlCursorScope scope,
                MappedTableStorage mapped,
                SqlStatementDeadline deadline,
                boolean consistent,
                boolean empty) {
            this.scope = scope;
            this.handle = scope.handle;
            this.mapped = mapped;
            this.deadline = deadline;
            this.clustered = mapped.clusteredIndex();
            this.consistent = consistent;
            this.empty = empty;
        }

        /**
         * 在子类全部字段完成校验后创建 ReadView，避免子类构造失败遗留已登记的 RC view。
         */
        private void initializeReadView() {
            view = scope.readView(
                    consistent, empty);
        }

        /**
         * 拉取下一条完整逻辑行，并在任何扫描失败前使旧 row view 失效。
         *
         * <ol>
         *     <li>校验 cursor 未关闭，推进 generation 并清除旧 current。</li>
         *     <li>委托具体 access cursor 完成短 MTR/MVCC/current-read 定位。</li>
         *     <li>EOF 返回 false；非空记录成为本 generation 的唯一 current。</li>
         * </ol>
         *
         * @return 有新当前行时为 {@code true}，EOF 时为 {@code false}
         * @throws SqlStorageException cursor 已关闭或底层读取失败时抛出
         */
        @Override
        public final boolean advance() {
            // 1、旧视图先失效；即使下游扫描失败也不能继续读取上一行。
            requireOpen();
            generation++;
            current = null;
            // 2、具体 cursor 返回前不得保留 page latch/fix。
            LogicalRecord next;
            try {
                next = nextRecord();
            } catch (RuntimeException readFailure) {
                throw adapt(
                        "advance SQL storage cursor failed",
                        readFailure);
            }
            // 3、Java null 只在 adapter 内部表达 EOF，不进入 SQL NULL 值域。
            if (next == null) {
                return false;
            }
            current = next;
            return true;
        }

        /**
         * 返回当前 generation 的 cursor-owned 视图。
         *
         * @return 下一次 advance/close 前有效的完整逻辑行视图
         * @throws SqlStorageException 尚未成功 advance 或 cursor 已关闭时抛出
         */
        @Override
        public final SqlRowView current() {
            requireOpen();
            if (current == null) {
                throw new SqlStorageException(
                        "SQL cursor has no current row");
            }
            return new EngineSqlRowView(
                    this, generation, current);
        }

        /**
         * 幂等关闭 cursor 局部资源；RC view 与 operation lease 由外层 statement scope 统一释放。
         *
         * <ol>
         *     <li>先发布 closed/generation，使任何旧 row view 立即失效。</li>
         *     <li>释放本 access cursor 的批次/列表等局部资源。</li>
         *     <li>局部失败向 scope/Executor 传播；不得自行关闭共享 ReadView 或解锁 transaction handle。</li>
         * </ol>
         */
        @Override
        public final void close() {
            if (closed) {
                return;
            }
            // 1、先发布 closed/generation，清理失败后旧 row view 也绝不能恢复可用。
            closed = true;
            generation++;
            current = null;
            RuntimeException failure = null;
            try {
                closeCursor();
            } catch (RuntimeException closeFailure) {
                failure = adapt(
                        "close SQL storage cursor resources failed",
                        closeFailure);
            }
            if (failure != null) {
                throw failure;
            }
        }

        /** 返回下一条完整逻辑行；EOF 用包内 Java null 表示。 */
        protected abstract LogicalRecord nextRecord();

        /** 子类可释放批次缓冲等非事务资源；默认无资源。 */
        protected void closeCursor() {
        }

        /** 验证 cursor 仍打开。 */
        private void requireOpen() {
            if (closed) {
                throw new SqlStorageException(
                        "SQL storage cursor is closed");
            }
        }

        /**
         * 验证 row view 仍对应当前 generation/record。
         */
        private void requireCurrent(
                long expectedGeneration,
                LogicalRecord expectedRecord) {
            requireOpen();
            if (generation != expectedGeneration
                    || current != expectedRecord) {
                throw new SqlStorageException(
                        "SQL row view is no longer current");
            }
        }
    }

    /**
     * cursor-owned 行视图；每列 external value 仅在真正投影读取时 hydrate 一次。
     */
    private final class EngineSqlRowView implements SqlRowView {
        /** 所属 cursor，用于 generation 校验和 deadline。 */
        private final AbstractEngineSqlStorageCursor cursor;
        /** 创建视图时的 generation。 */
        private final long generation;
        /** 已脱离 page latch/fix 的完整逻辑行。 */
        private final LogicalRecord record;
        /** 每列惰性 SQL value cache；Java null 仅表示尚未读取。 */
        private final SqlValue[] values;
        /** 每列已 hydrate 的 storage value；Java null 仅表示尚未解析 external 引用。 */
        private final ColumnValue[] hydratedValues;

        private EngineSqlRowView(
                AbstractEngineSqlStorageCursor cursor,
                long generation,
                LogicalRecord record) {
            this.cursor = cursor;
            this.generation = generation;
            this.record = record;
            this.values = new SqlValue[
                    record.columnValues().size()];
            this.hydratedValues = new ColumnValue[
                    record.columnValues().size()];
        }

        @Override
        public int width() {
            requireCurrent();
            return record.columnValues().size();
        }

        @Override
        public SqlValue valueAt(int ordinal) {
            requireOrdinal(ordinal);
            try {
                SqlValue cached = values[ordinal];
                if (cached != null) {
                    return cached;
                }
                ColumnValue value = storageValueAt(ordinal);
                SqlValue converted = toSqlValue(
                        value, cursor.mapped.table()
                                .columns().get(ordinal).type());
                values[ordinal] = converted;
                return converted;
            } catch (RuntimeException valueFailure) {
                throw adapt(
                        "read SQL cursor row value failed for column "
                                + ordinal,
                        valueFailure);
            }
        }

        @Override
        public boolean isNullAt(int ordinal) {
            requireOrdinal(ordinal);
            return record.columnValues().get(ordinal)
                    instanceof ColumnValue.NullValue;
        }

        @Override
        public int compareLiteral(
                int ordinal, SqlValue literal) {
            requireOrdinal(ordinal);
            try {
                if (literal == null
                        || literal instanceof SqlValue.NullValue) {
                    throw new SqlStorageException(
                            "row comparison requires non-null SQL literal");
                }
                ColumnValue left = storageValueAt(ordinal);
                if (left instanceof ColumnValue.NullValue) {
                    throw new SqlStorageException(
                            "row comparison cannot compare SQL NULL");
                }
                ColumnValue right = toColumnValue(
                        literal, cursor.clustered.schema()
                                .column(ordinal).type());
                return compareExpressionValues(
                        left, right, cursor.clustered, ordinal);
            } catch (RuntimeException comparisonFailure) {
                throw adapt(
                        "compare SQL cursor row value failed for column "
                                + ordinal,
                        comparisonFailure);
            }
        }

        /**
         * 返回可交给 type codec/comparator 的列值；只有该列真正取值或比较时才 hydrate LOB。
         */
        private ColumnValue storageValueAt(int ordinal) {
            ColumnValue cached = hydratedValues[ordinal];
            if (cached != null) {
                return cached;
            }
            ColumnValue value =
                    record.columnValues().get(ordinal);
            if (value instanceof ColumnValue.ExternalValue external) {
                value = hydrateExternalValue(
                        external, cursor.clustered,
                        cursor.deadline, ordinal);
            }
            hydratedValues[ordinal] = value;
            return value;
        }

        /** 验证视图有效且 ordinal 属于 exact row schema。 */
        private void requireOrdinal(int ordinal) {
            requireCurrent();
            if (ordinal < 0
                    || ordinal >= record.columnValues().size()) {
                throw new SqlStorageException(
                        "SQL row-view ordinal is outside schema");
            }
        }

        private void requireCurrent() {
            cursor.requireCurrent(generation, record);
        }
    }

    /**
     * 比较已完成 LOB hydration 的表达式值。普通类型继续使用索引/Record 的保序 codec；
     * LOB 因完整 payload 可能超过页内 256B 上限，必须在无 page latch 的 Executor 阶段直接按
     * exact charset/collation 比较逻辑字节。JSON v1 按项目既有“严格 UTF-8 文本”简化为二进制字节序，
     * 不声称实现 MySQL binary JSON 类型优先级。
     *
     * @param left 当前完整逻辑行的非 NULL、非 external 列值
     * @param right Binder 已转换到 exact column type 的非 NULL literal
     * @param clustered 当前表的 exact clustered schema
     * @param ordinal 待比较列在 exact table version 中的位置
     * @return 左值小于、等于或大于右值时的负数、零或正数
     * @throws SqlStorageException LOB hydration 后类型、字符或长度证据非法时抛出
     */
    private int compareExpressionValues(
            ColumnValue left, ColumnValue right,
            BTreeIndex clustered, int ordinal) {
        ColumnType type =
                clustered.schema().column(ordinal).type();
        return switch (type.typeId()) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT ->
                    compareLobPayload(
                            left, right, type, true);
            case TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON ->
                    compareLobPayload(
                            left, right, type, false);
            default -> {
                IndexKeyDef oneColumn = new IndexKeyDef(
                        clustered.indexId(),
                        List.of(new KeyPartDef(
                                new ColumnId(ordinal),
                                KeyOrder.ASC, 0)));
                yield predicateComparator.compare(
                        new SearchKey(List.of(left)),
                        new SearchKey(List.of(right)),
                        oneColumn, clustered.schema());
            }
        };
    }

    /**
     * 对两个已物化 LOB 逻辑值执行完整 payload 比较，不再施加记录页的 inline 编码上限。
     *
     * @param left 已 hydrate 的非 NULL LOB 列值
     * @param right Binder 已转换的同类型非 NULL literal
     * @param type exact Record column type
     * @param useColumnCollation TEXT family 为 true，BLOB/JSON v1 二进制文本为 false
     * @return 按 exact collation 或二进制字节序得到的比较结果
     */
    private int compareLobPayload(
            ColumnValue left, ColumnValue right,
            ColumnType type, boolean useColumnCollation) {
        LobCodec codec = (LobCodec)
                predicateTypeCodecs.codecFor(type);
        byte[] leftBytes =
                codec.logicalBytesForStorage(left, type);
        byte[] rightBytes =
                codec.logicalBytesForStorage(right, type);
        CollationStrategy collation = useColumnCollation
                ? predicateTypeCodecs.collationFor(
                        type.charset(), type.collation())
                : BinaryCollation.INSTANCE;
        return collation.compare(
                leftBytes, 0, leftBytes.length,
                rightBytes, 0, rightBytes.length);
    }

    /**
     * point cursor 在第一次 advance 执行一次聚簇/唯一二级 MVCC 定位。
     */
    private final class PointSqlStorageCursor
            extends AbstractEngineSqlStorageCursor {
        private final PhysicalPointAccess access;
        private boolean consumed;

        private PointSqlStorageCursor(
                EngineSqlCursorScope scope,
                MappedTableStorage mapped,
                PhysicalPointAccess access,
                SqlStatementDeadline deadline) {
            super(scope, mapped, deadline, true, false);
            this.access = access;
        }

        /**
         * 执行一次 point 定位。
         *
         * <ol>
         *     <li>消费单次定位额度，后续调用直接 EOF。</li>
         *     <li>按 access index exact codec 构造 search key。</li>
         *     <li>RU 读取 current non-marked 聚簇版本，unique secondary 必须回表并保持唯一。</li>
         *     <li>RR/RC 使用 cursor 已登记 ReadView 读取聚簇或唯一二级可见版本。</li>
         * </ol>
         *
         * @return 完整聚簇逻辑行；未找到、不可见或已消费时为 Java {@code null}
         * @throws SqlStorageException metadata、key codec、MVCC/undo 或唯一性证据失败时抛出
         */
        @Override
        protected LogicalRecord nextRecord() {
            // 1、point access 只执行一次；后续 advance 稳定返回 EOF。
            if (consumed) {
                return null;
            }
            consumed = true;
            // 2、按 access index 的 exact codec 构造 typed search key。
            BTreeIndex index =
                    mapped.index(access.accessIndexId());
            SearchKey key = new SearchKey(
                    toKeyValues(access.keyValues(), index));
            if (view == null) {
                // 3、RU 不创建 ReadView；unique secondary 仍回聚簇并拒绝多 live identity。
                if (access.accessKind()
                        == PointAccessKind.CLUSTERED_PRIMARY) {
                    return engine.mvccReader()
                            .readUncommitted(clustered, key)
                            .orElse(null);
                }
                List<LogicalRecord> rows =
                        scanReadUncommittedRaw(
                                mapped, access.accessIndexId(),
                                equalityRange(access.keyValues()),
                                deadline);
                if (rows.size() > 1) {
                    throw new SqlStorageException(
                            "multiple current rows for logical unique secondary key");
                }
                return rows.isEmpty() ? null : rows.getFirst();
            }
            // 4、RR/RC 共用已登记 view，point 结果只携带完整聚簇逻辑值。
            Optional<LogicalRecord> record =
                    access.accessKind()
                            == PointAccessKind.CLUSTERED_PRIMARY
                            ? engine.mvccReader().read(
                                    view, clustered, key)
                            : engine.secondaryMvccReader()
                                    .readUnique(
                                            view,
                                            mapped.tableIndexes(),
                                            mapped.tableIndexes()
                                                    .requireSecondary(
                                                            access.accessIndexId()),
                                            key);
            return record.orElse(null);
        }
    }

    /**
     * logical secondary prefix 服务当前返回稳定完整行集合；cursor 逐行暴露且让 RC view
     * 一直存活到最后一行投影完成。后续可把底层 reader 继续细化为分页 cursor。
     */
    private final class SecondaryPrefixSqlStorageCursor
            extends AbstractEngineSqlStorageCursor {
        private final EngineSqlTransactionHandle handle;
        private final PhysicalSecondaryPrefixAccess access;
        private List<LogicalRecord> rows;
        private int index;

        private SecondaryPrefixSqlStorageCursor(
                EngineSqlCursorScope scope,
                MappedTableStorage mapped,
                PhysicalSecondaryPrefixAccess access,
                SqlStatementDeadline deadline) {
            super(scope, mapped, deadline,
                    access.lockMode()
                            == SelectLockMode.CONSISTENT,
                    false);
            this.handle = scope.handle;
            this.access = access;
        }

        /**
         * 首次拉取时加载底层稳定列表，随后逐行发布。
         *
         * <ol>
         *     <li>只在第一次调用选择 reader 并加载完整行列表。</li>
         *     <li>按稳定顺序返回当前元素，列表耗尽后返回 EOF。</li>
         * </ol>
         *
         * @return 下一条完整聚簇逻辑行，EOF 时为 Java {@code null}
         * @throws SqlStorageException 底层 secondary reader 或回表失败时抛出
         */
        @Override
        protected LogicalRecord nextRecord() {
            // 1、底层 reader 首次调用形成稳定列表，后续 advance 不重复访问存储。
            if (rows == null) {
                rows = loadRows();
            }
            // 2、adapter 边界逐行发布；EOF 不泄露列表或 record 引用。
            return index < rows.size()
                    ? rows.get(index++) : null;
        }

        /**
         * 按 read intent 选择 locking、RU current 或 RR/RC MVCC secondary reader。
         *
         * @return 已回聚簇、脱离 page latch/fix 的稳定完整行列表
         * @throws SqlStorageException metadata、锁等待、MVCC 或容量失败时抛出
         */
        private List<LogicalRecord> loadRows() {
            BTreeIndex index =
                    mapped.index(access.accessIndexId());
            SearchKey logicalKey = new SearchKey(
                    toKeyValues(
                            access.logicalKeyValues(), index));
            SecondaryIndexMetadata secondary =
                    mapped.tableIndexes().requireSecondary(
                            access.accessIndexId());
            if (access.lockMode()
                    != SelectLockMode.CONSISTENT) {
                return engine.secondaryCurrentReadService()
                        .readRange(
                                handle.transaction,
                                mapped.tableIndexes(),
                                secondary, logicalKey,
                                access.lockMode()
                                        == SelectLockMode.FOR_SHARE
                                        ? BTreeCurrentReadMode.FOR_SHARE
                                        : BTreeCurrentReadMode.FOR_UPDATE,
                                deadline.cap(
                                        operationTimeout,
                                        "secondary-prefix locking cursor"));
            }
            if (view == null) {
                return scanReadUncommittedRaw(
                        mapped, access.accessIndexId(),
                        equalityRange(
                                access.logicalKeyValues()),
                        deadline);
            }
            return engine.secondaryMvccReader().readRange(
                    view, mapped.tableIndexes(),
                    secondary, logicalKey);
        }
    }

    /**
     * 通用 range cursor：每次只持有一个 256-candidate 批次，逐候选回聚簇并返回完整行。
     */
    private final class RangeSqlStorageCursor
            extends AbstractEngineSqlStorageCursor {
        private final EngineSqlTransactionHandle handle;
        private final PhysicalRangeAccess rangeAccess;
        private final BTreeIndex access;
        private final SecondaryIndexMetadata secondary;
        private final ArrayList<SearchKey> identities =
                new ArrayList<>();
        private List<BTreeLookupResult> batch = List.of();
        private int batchIndex;
        private SearchKey continuation;
        private int physicalCandidates;
        private boolean exhausted;
        private final TransactionId lockOwner;
        private final BTreeCurrentReadMode currentReadMode;

        private RangeSqlStorageCursor(
                EngineSqlCursorScope scope,
                MappedTableStorage mapped,
                PhysicalRangeAccess rangeAccess,
                SqlStatementDeadline deadline) {
            super(scope, mapped, deadline,
                    rangeAccess.lockMode()
                            == SelectLockMode.CONSISTENT,
                    rangeAccess.empty());
            this.handle = scope.handle;
            this.rangeAccess = rangeAccess;
            this.access = mapped.index(
                    rangeAccess.accessIndexId());
            this.secondary = access.clustered()
                    ? null : mapped.tableIndexes()
                            .requireSecondary(
                                    rangeAccess.accessIndexId());
            if (!rangeAccess.empty()
                    && rangeAccess.lockMode()
                    != SelectLockMode.CONSISTENT) {
                this.lockOwner = engine.transactionManager()
                        .assignWriteId(handle.transaction);
                this.currentReadMode =
                        rangeAccess.lockMode()
                                == SelectLockMode.FOR_SHARE
                                ? BTreeCurrentReadMode.FOR_SHARE
                                : BTreeCurrentReadMode.FOR_UPDATE;
            } else {
                this.lockOwner = null;
                this.currentReadMode = null;
            }
            this.exhausted = rangeAccess.empty();
        }

        /**
         * 逐批扫描并跳过不可见、重复或重定位消失的候选。
         *
         * <ol>
         *     <li>当前批次耗尽时按 continuation 有界载入下一批。</li>
         *     <li>逐候选回聚簇并去重，直到得到一行或全局 EOF。</li>
         * </ol>
         *
         * @return 下一条完整聚簇逻辑行，EOF 时为 Java {@code null}
         * @throws SqlStorageException 扫描、MVCC/undo、锁等待或容量失败时抛出
         */
        @Override
        protected LogicalRecord nextRecord() {
            // 1、批次耗尽后才取下一批，单 cursor 最多持有 256 个 physical candidates。
            while (true) {
                if (batchIndex >= batch.size()) {
                    if (exhausted || !loadBatch()) {
                        return null;
                    }
                }
                BTreeLookupResult candidate =
                        batch.get(batchIndex++);
                // 2、逐候选回聚簇/去重；不可见或已删除候选在 adapter 内跳过。
                LogicalRecord row = resolveCandidate(candidate);
                if (row != null) {
                    return row;
                }
            }
        }

        /**
         * 读取下一物理批次；短批在消费完成后标记 EOF，continuation 使用完整 physical key。
         *
         * @return 成功载入非空批次时为 {@code true}，已 EOF 时为 {@code false}
         * @throws SqlStorageException deadline、扫描、锁等待或 candidate 容量失败时抛出
         */
        private boolean loadBatch() {
            BTreeScanRange scanRange = toBTreeRange(
                    rangeAccess.indexRange(), access,
                    continuation, RANGE_SCAN_BATCH_SIZE);
            List<BTreeLookupResult> loaded;
            if (rangeAccess.lockMode()
                    == SelectLockMode.CONSISTENT) {
                deadline.remaining(
                        "SQL range cursor physical batch");
                MiniTransaction mtr = engine
                        .miniTransactionManager()
                        .beginReadOnly();
                try {
                    loaded = access.clustered()
                            ? engine.btreeService()
                                    .scanClusteredIncludingDeleted(
                                            mtr, access, scanRange)
                            : engine.btreeService()
                                    .scanIncludingDeleted(
                                            mtr, access, scanRange);
                    engine.miniTransactionManager()
                            .commit(mtr);
                } catch (RuntimeException scanFailure) {
                    rollbackMtr(mtr, scanFailure);
                    throw scanFailure;
                }
            } else {
                BTreeCurrentReadRequest request =
                        new BTreeCurrentReadRequest(
                                lockOwner,
                                handle.transaction
                                        .isolationLevel(),
                                deadline.cap(
                                        operationTimeout,
                                        "SQL locking range cursor"),
                                RANGE_RELOCATION_RETRIES);
                loaded = engine.btreeCurrentReadService()
                        .lockRange(
                                access, scanRange,
                                request, currentReadMode);
            }
            if (loaded.isEmpty()) {
                exhausted = true;
                batch = List.of();
                batchIndex = 0;
                return false;
            }
            physicalCandidates += loaded.size();
            requirePhysicalCapacity(
                    physicalCandidates, mapped, access);
            continuation = keyFromRow(
                    loaded.getLast().record()
                            .columnValues(), access);
            exhausted = loaded.size()
                    < RANGE_SCAN_BATCH_SIZE;
            batch = List.copyOf(loaded);
            batchIndex = 0;
            return true;
        }

        /**
         * 把 access candidate 转成唯一聚簇逻辑行；锁等待发生前 access scan 已释放 page 资源。
         *
         * @param candidate 当前批次内、已脱离 page latch/fix 的 access record
         * @return 可发布的完整聚簇逻辑行；重复、不可见或重定位消失时为 Java {@code null}
         * @throws SqlStorageException 聚簇锁等待、MVCC/undo 或 identity 比较失败时抛出
         */
        private LogicalRecord resolveCandidate(
                BTreeLookupResult candidate) {
            SearchKey identity = access.clustered()
                    ? keyFromRow(
                            candidate.record()
                                    .columnValues(), clustered)
                    : secondary.layout().clusterKey(
                            candidate.record());
            if (containsIdentity(
                    identities, identity, clustered)) {
                return null;
            }
            identities.add(identity);

            if (rangeAccess.lockMode()
                    != SelectLockMode.CONSISTENT) {
                if (access.clustered()) {
                    return candidate.record();
                }
                BTreeCurrentReadRequest request =
                        new BTreeCurrentReadRequest(
                                lockOwner,
                                handle.transaction
                                        .isolationLevel(),
                                deadline.cap(
                                        operationTimeout,
                                        "SQL cursor clustered row lock"),
                                RANGE_RELOCATION_RETRIES);
                return engine.btreeCurrentReadService()
                        .lockPoint(
                                clustered, identity,
                                request, currentReadMode)
                        .map(BTreeLookupResult::record)
                        .orElse(null);
            }
            if (view == null) {
                return engine.mvccReader()
                        .readUncommitted(
                                clustered, identity)
                        .orElse(null);
            }
            return engine.mvccReader()
                    .read(view, clustered, identity)
                    .orElse(null);
        }

        @Override
        protected void closeCursor() {
            batch = List.of();
            batchIndex = 0;
            identities.clear();
        }
    }

    /**
     * 范围 UPDATE 先完成 FOR UPDATE scan 与 identity 物化，再在一个 statement guard 内逐点应用 patch。
     */
    @Override
    public SqlWriteOutcome updateRange(SqlTransactionHandle transaction, PhysicalRangeUpdate statement,
                                       SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("range UPDATE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException(
                    "physical range UPDATE/deadline must not be null");
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
    public SqlWriteOutcome deleteRange(SqlTransactionHandle transaction, PhysicalRangeDelete statement,
                                       SqlStatementDeadline deadline) {
        rejectRecoveryExportWrite("range DELETE");
        if (statement == null || deadline == null) {
            throw new DatabaseValidationException(
                    "physical range DELETE/deadline must not be null");
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
     * RU cursor 的原始候选读取：只负责分页、回聚簇和 identity 去重，不执行 residual 或公开投影。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按完整 physical continuation 读取 256 条 including-deleted access candidates。</li>
     *     <li>批次 MTR 提交后再提取聚簇 identity，保证 page latch/fix 不跨回表读取。</li>
     *     <li>逐 identity 读取瞬间 current non-marked 聚簇版本并去重，最多检查 16384 physical candidates。</li>
     *     <li>返回不含 storage page reference 的完整 LogicalRecord 集；Filter/Project 由 Executor 后续完成。</li>
     * </ol>
     *
     * @param mapped exact table/index mapping
     * @param accessIndexId optimizer 选择的访问索引
     * @param range physical range
     * @param deadline statement absolute deadline
     * @return 当前版本候选行；不按 SQL residual 过滤
     */
    private List<LogicalRecord> scanReadUncommittedRaw(
            MappedTableStorage mapped, long accessIndexId,
            IndexRange range, SqlStatementDeadline deadline) {
        BTreeIndex access = mapped.index(accessIndexId);
        BTreeIndex clustered = mapped.clusteredIndex();
        SecondaryIndexMetadata secondary = access.clustered()
                ? null : mapped.tableIndexes()
                        .requireSecondary(accessIndexId);
        ArrayList<SearchKey> identities = new ArrayList<>();
        ArrayList<LogicalRecord> rows = new ArrayList<>();
        SearchKey continuation = null;
        int physicalCandidates = 0;
        while (true) {
            // 1、每批只在短 MTR 中读取物理候选，continuation 不省略 secondary clustered suffix。
            deadline.remaining(
                    "read-uncommitted raw cursor batch");
            BTreeScanRange batchRange = toBTreeRange(
                    range, access, continuation,
                    RANGE_SCAN_BATCH_SIZE);
            MiniTransaction mtr = engine
                    .miniTransactionManager().beginReadOnly();
            List<BTreeLookupResult> batch;
            try {
                batch = access.clustered()
                        ? engine.btreeService()
                                .scanClusteredIncludingDeleted(
                                        mtr, access, batchRange)
                        : engine.btreeService()
                                .scanIncludingDeleted(
                                        mtr, access, batchRange);
                engine.miniTransactionManager().commit(mtr);
            } catch (RuntimeException scanFailure) {
                rollbackMtr(mtr, scanFailure);
                throw scanFailure;
            }
            if (batch.isEmpty()) {
                return List.copyOf(rows);
            }
            // 2/3、page 资源已释放，回聚簇只保留当前逻辑行值。
            physicalCandidates += batch.size();
            requirePhysicalCapacity(
                    physicalCandidates, mapped, access);
            for (BTreeLookupResult candidate : batch) {
                SearchKey identity = access.clustered()
                        ? keyFromRow(
                                candidate.record()
                                        .columnValues(), clustered)
                        : secondary.layout()
                                .clusterKey(candidate.record());
                if (containsIdentity(
                        identities, identity, clustered)) {
                    continue;
                }
                identities.add(identity);
                engine.mvccReader()
                        .readUncommitted(clustered, identity)
                        .ifPresent(rows::add);
            }
            // 4、原始 cursor 不执行 residual/result-row 上限；Filter 后由 Executor 限制 4096 输出行。
            continuation = keyFromRow(
                    batch.getLast().record()
                            .columnValues(), access);
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
            IndexRange range, PredicateSet predicates, SelectLockMode lockMode,
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

    private BTreeScanRange toBTreeRange(IndexRange range, BTreeIndex access,
                                        SearchKey continuation, int limit) {
        Optional<SearchKey> lower = continuation == null
                ? range.lower().map(endpoint -> endpointKey(endpoint, access))
                : Optional.of(continuation);
        boolean lowerInclusive = continuation == null
                ? range.lower().map(RangeEndpoint::inclusive).orElse(true) : false;
        Optional<SearchKey> upper = range.upper().map(endpoint -> endpointKey(endpoint, access));
        boolean upperInclusive = range.upper().map(RangeEndpoint::inclusive).orElse(true);
        return BTreeScanRange.of(lower, lowerInclusive, upper, upperInclusive, limit);
    }

    private SearchKey endpointKey(RangeEndpoint endpoint, BTreeIndex access) {
        return new SearchKey(toKeyValues(endpoint.keyValues(), access));
    }

    /** 把完整 logical equality key 表达为双侧闭合前缀范围。 */
    private static IndexRange equalityRange(List<SqlValue> values) {
        RangeEndpoint endpoint = new RangeEndpoint(values, true);
        return new IndexRange(Optional.of(endpoint), Optional.of(endpoint));
    }

    /**
     * 使用 Record codec 的 exact type/collation 比较语义求值完整 residual。
     */
    private boolean matchesPredicates(
            LogicalRecord row, PredicateSet predicates,
            BTreeIndex clustered) {
        return expressionEvaluator.matches(
                predicates.condition(),
                new PredicateSqlRowView(row, clustered));
    }

    /**
     * range DML 在物化 identity 前使用的瞬时 row view。当前 canonical residual 只需要 NULL
     * 探针和 column-literal comparator；valueAt 若被未来 scalar 规则调用会显式失败，避免无
     * ReadView/deadline 的 LOB hydration 悄悄进入 DML predicate 阶段。
     */
    private final class PredicateSqlRowView
            implements SqlRowView {
        private final LogicalRecord record;
        private final BTreeIndex clustered;

        private PredicateSqlRowView(
                LogicalRecord record, BTreeIndex clustered) {
            this.record = record;
            this.clustered = clustered;
        }

        @Override
        public int width() {
            return record.columnValues().size();
        }

        @Override
        public SqlValue valueAt(int ordinal) {
            throw new SqlStorageException(
                    "range DML predicate row does not expose projected values");
        }

        @Override
        public boolean isNullAt(int ordinal) {
            requirePredicateOrdinal(ordinal);
            return record.columnValues().get(ordinal)
                    instanceof ColumnValue.NullValue;
        }

        @Override
        public int compareLiteral(
                int ordinal, SqlValue literal) {
            requirePredicateOrdinal(ordinal);
            ColumnValue left =
                    record.columnValues().get(ordinal);
            if (left instanceof ColumnValue.NullValue
                    || literal == null
                    || literal instanceof SqlValue.NullValue) {
                throw new SqlStorageException(
                        "predicate comparator requires non-null operands");
            }
            ColumnValue right = toColumnValue(
                    literal,
                    clustered.schema().column(ordinal).type());
            IndexKeyDef oneColumn = new IndexKeyDef(
                    clustered.indexId(),
                    List.of(new KeyPartDef(
                            new ColumnId(ordinal),
                            KeyOrder.ASC, 0)));
            return predicateComparator.compare(
                    new SearchKey(List.of(left)),
                    new SearchKey(List.of(right)),
                    oneColumn, clustered.schema());
        }

        private void requirePredicateOrdinal(int ordinal) {
            if (ordinal < 0
                    || ordinal >= record.columnValues().size()) {
                throw new SqlStorageException(
                        "range DML predicate ordinal is outside row schema");
            }
        }
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
            if (handle.cursorActive) {
                throw new SqlTransactionStateException(
                        "SQL transaction handle has an active cursor");
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
            if (handle.cursorActive) {
                throw new SqlTransactionStateException(
                        "SQL transaction handle has an active cursor");
            }
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

    /**
     * 为 cursor 当前行按需 hydrate 单个 external value；ReadView 和 operation lease 由所属
     * statement cursor scope 持有。
     *
     * @param external 当前逻辑行中的外置引用
     * @param index exact clustered schema
     * @param deadline statement absolute deadline
     * @param ordinal 待投影 table column ordinal
     * @return 不含 LobReference 的内联 ColumnValue
     * @throws SqlStorageException LOB 读取、CRC、deadline 或 MTR 收敛失败时抛出
     */
    private ColumnValue hydrateExternalValue(
            ColumnValue.ExternalValue external,
            BTreeIndex index,
            SqlStatementDeadline deadline,
            int ordinal) {
        deadline.remaining(
                "cursor external LOB hydration for column "
                        + ordinal);
        MiniTransaction mtr = engine.miniTransactionManager()
                .beginReadOnly();
        try {
            ColumnType type =
                    index.schema().column(ordinal).type();
            ColumnValue value = engine.lobStorage()
                    .read(mtr, type, external);
            engine.miniTransactionManager().commit(mtr);
            deadline.remaining(
                    "cursor external LOB hydration completion for column "
                            + ordinal);
            return value;
        } catch (RuntimeException hydrateFailure) {
            rollbackMtr(mtr, hydrateFailure);
            throw adapt(
                    "cursor external LOB hydration failed for column "
                            + ordinal,
                    hydrateFailure);
        }
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
