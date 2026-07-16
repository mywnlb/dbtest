package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPrimaryPointSelect;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.storage.*;
import cn.zhangyis.db.sql.executor.storage.exception.*;
import cn.zhangyis.db.storage.api.dml.*;
import cn.zhangyis.db.storage.btree.BTreeIndex;
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
    private static final BigInteger TWO = BigInteger.valueOf(2);
    private final StorageEngine engine;
    private final DictionaryStorageMetadataMapper mapper;
    /** 行锁等待与 handle 并发占用都必须有界；Session 可用 statement timeout 构造此 adapter。 */
    private final Duration operationTimeout;

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

    /** 显式映射 SQL isolation；v1 只开放已有 RR/RC ReadView 语义。 */
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
     */
    @Override
    public SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement) {
        if (statement == null) throw new DatabaseValidationException("bound INSERT must not be null");
        return withActive(transaction, handle -> {
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex index = mapped.clusteredIndex();
            List<ColumnValue> values = toColumnValues(statement.values(), index);
            SearchKey key = keyFromRow(values, index);
            LogicalRecord record = new LogicalRecord(index.schema().schemaVersion(), values, false,
                    RecordType.CONVENTIONAL);
            DmlStatementGuard guard = engine.dmlService().beginStatement(handle.transaction, index);
            try {
                DmlWriteResult result = engine.dmlService().insert(new ClusteredInsertCommand(handle.transaction,
                        index, key, record, statement.table().id().value(), mapped.lobSegment(), operationTimeout));
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

    /** RR 复用事务 ReadView，RC 每语句 finally 注销；整行 LOB hydrate 完成后才投影，失败不返回 partial row。 */
    @Override
    public Optional<SqlRow> selectPoint(SqlTransactionHandle transaction, BoundPrimaryPointSelect statement) {
        if (statement == null) throw new DatabaseValidationException("bound SELECT must not be null");
        return withActive(transaction, handle -> {
            MappedTableStorage mapped = mapper.map(statement.table());
            BTreeIndex index = mapped.clusteredIndex();
            SearchKey key = new SearchKey(toKeyValues(statement.keyValues(), index));
            ReadViewManager views = engine.transactionManager().readViewManager();
            ReadView view = views.openReadView(handle.transaction);
            Optional<LogicalRecord> record = Optional.empty();
            RuntimeException failure = null;
            try {
                record = engine.mvccReader().read(view, index, key);
            } catch (RuntimeException readFailure) {
                failure = readFailure;
            }
            if (handle.transaction.isolationLevel() == IsolationLevel.READ_COMMITTED) {
                try { views.closeReadView(view); }
                catch (RuntimeException closeFailure) {
                    if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) throw adapt("primary-point MVCC read failed", failure);
            if (record.isEmpty()) return Optional.empty();

            List<ColumnValue> hydrated = hydrateExternalValues(record.orElseThrow().columnValues(), index);
            List<SqlValue> projected = new ArrayList<>(statement.projectionOrdinals().size());
            for (int ordinal : statement.projectionOrdinals()) {
                projected.add(toSqlValue(hydrated.get(ordinal), statement.table().columns().get(ordinal).type()));
            }
            return Optional.of(new SqlRow(projected));
        });
    }

    /** 首写事务走 DML durability/lock 收尾；未成功写入的事务直接完成内存生命周期且释放可能存在的行锁。 */
    @Override
    public SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request) {
        if (request == null) throw new DatabaseValidationException("SQL commit request must not be null");
        return withActive(transaction, handle -> {
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

    /** 有 undo 使用 DD resolver 做跨表 full rollback；无 undo 走轻量生命周期，终态后才释放锁。 */
    @Override
    public SqlRollbackOutcome rollback(SqlTransactionHandle transaction) {
        return withActive(transaction, handle -> {
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

    private <T> T withActive(SqlTransactionHandle candidate,
                             java.util.function.Function<EngineSqlTransactionHandle, T> action) {
        requireEngineOpen();
        EngineSqlTransactionHandle handle = requireOwned(candidate);
        boolean acquired;
        try {
            acquired = handle.operationLock.tryLock(operationTimeout.toNanos(), TimeUnit.NANOSECONDS);
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
        if (keys.size() != index.keyDef().parts().size()) {
            throw new SqlStorageException("bound key width differs from mapped primary key");
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

    private List<ColumnValue> hydrateExternalValues(List<ColumnValue> source, BTreeIndex index) {
        ArrayList<ColumnValue> result = new ArrayList<>(source);
        for (int ordinal = 0; ordinal < result.size(); ordinal++) {
            if (!(result.get(ordinal) instanceof ColumnValue.ExternalValue external)) continue;
            MiniTransaction mtr = engine.miniTransactionManager().beginReadOnly();
            try {
                ColumnType type = index.schema().column(ordinal).type();
                ColumnValue value = engine.lobStorage().read(mtr, type, external);
                engine.miniTransactionManager().commit(mtr);
                result.set(ordinal, value);
            } catch (RuntimeException hydrateFailure) {
                rollbackMtr(mtr, hydrateFailure);
                throw adapt("external LOB hydration failed for column " + ordinal, hydrateFailure);
            }
        }
        return List.copyOf(result);
    }

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

    private static SqlStorageException adapt(String message, RuntimeException error) {
        return error instanceof SqlStorageException storage ? storage : new SqlStorageException(message, error);
    }
}
