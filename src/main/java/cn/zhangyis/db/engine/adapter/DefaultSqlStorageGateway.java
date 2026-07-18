package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseFailureClassifier;
import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.binder.bound.PointAccessKind;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.*;
import cn.zhangyis.db.storage.api.dml.*;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadMode;
import cn.zhangyis.db.storage.engine.EngineState;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.type.ColumnValue;
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
        if (engine == null || mapper == null || operationTimeout == null
                || operationTimeout.isZero() || operationTimeout.isNegative()) {
            throw new DatabaseValidationException("gateway engine/mapper/positive operation timeout required");
        }
        this.engine = engine;
        this.mapper = mapper;
        this.operationTimeout = operationTimeout;
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
            case READ_COMMITTED -> IsolationLevel.READ_COMMITTED;
            case REPEATABLE_READ -> IsolationLevel.REPEATABLE_READ;
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
