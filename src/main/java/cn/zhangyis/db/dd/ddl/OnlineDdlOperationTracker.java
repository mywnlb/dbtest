package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个Online DDL的进程内可观察投影。短显式锁只保护标量和有界字符串，不在锁内访问catalog、row-log、
 * gate、MDL或物理页；prepare Condition只等待coordinator在锁外append marker后的结果通知。
 */
public final class OnlineDdlOperationTracker {

    /** pre-prepare cancel与marker append之间的原子handoff状态。 */
    private enum PrepareState {
        /** cancel/coordinator尚未声明所有权。 */
        PREPARE_OPEN,
        /** coordinator已取得所有权，正在锁外append marker。 */
        PREPARING_DURABLE,
        /** marker已确认durable，取消必须走repository CAS。 */
        DURABLE,
        /** cancel在任何durable resource前胜出。 */
        CANCELLED_BEFORE_PREPARE,
        /** append结果不确定，禁止把内存取消声称为成功。 */
        FAILED_CLOSED
    }

    /** tracker的immutable operation identity。 */
    private final OnlineDdlOperationIdentity identity;
    /** marker protocol决定的cancel capability。 */
    private final DdlExecutionProtocol protocol;
    /** 保护全部下列投影和prepare/terminal条件。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** prepare handoff或terminal结果改变时唤醒有界等待者。 */
    private final Condition changed = lock.newCondition();
    /** 注册时墙钟，仅用于诊断。 */
    private final long startedAtEpochMillis;

    private long epoch = 1;
    private long updatedAtEpochMillis;
    private PrepareState prepareState = PrepareState.PREPARE_OPEN;
    private OnlineDdlRuntimePhase runtimePhase = OnlineDdlRuntimePhase.REGISTERED;
    private OnlineDdlWaitReason waitReason = OnlineDdlWaitReason.NONE;
    private OnlineDdlTablePhase gatePhase = OnlineDdlTablePhase.ABSENT;
    private Optional<DdlLogPhase> durablePhase = Optional.empty();
    private DdlControlState controlState = DdlControlState.OPEN;
    private Optional<DdlCancellationReason> cancellationReason = Optional.empty();
    private long rowsScanned;
    private long batchesScanned;
    private OptionalLong estimatedRows = OptionalLong.empty();
    private Optional<String> lastClusteredKeyDigest = Optional.empty();
    private long candidateCount;
    private long rowLogBytes;
    private long maxRowLogBytes;
    private int abortReserveBytes;
    private long highestAppendedSequence;
    private long highestForcedSequence;
    private long rowLogGeneration;
    private long inFlightAdmissions;
    private long ioLeases;
    private long terminalRedoHighWater;
    private boolean retirementFencePresent;
    private boolean retirementSafe;
    private OnlineDdlTerminalResult terminalResult = OnlineDdlTerminalResult.NONE;
    private Optional<String> lastErrorCode = Optional.empty();
    private boolean forwardRecoveryRequired;

    /**
     * @param identity 已在initial/recovery分类点冻结的不可变身份
     * @param protocol marker将持久化或已经解码的执行协议
     */
    OnlineDdlOperationTracker(
            OnlineDdlOperationIdentity identity, DdlExecutionProtocol protocol) {
        if (identity == null || protocol == null) {
            throw new DatabaseValidationException("Online DDL tracker identity/protocol must not be null");
        }
        this.identity = identity;
        this.protocol = protocol;
        this.startedAtEpochMillis = System.currentTimeMillis();
        this.updatedAtEpochMillis = startedAtEpochMillis;
    }

    /**
     * coordinator原子取得marker append所有权；cancel已经在prepare前胜出时返回false且不得写任何durable resource。
     *
     * @return true表示调用方必须在锁外append marker并随后调用markDurablePrepared/failDurablePrepare
     * @throws DatabaseValidationException 重复或终态handoff表示coordinator生命周期错误时抛出
     */
    public boolean beginDurablePrepare() {
        lock.lock();
        try {
            if (prepareState == PrepareState.CANCELLED_BEFORE_PREPARE) {
                return false;
            }
            if (prepareState != PrepareState.PREPARE_OPEN) {
                throw new DatabaseValidationException(
                        "Online DDL durable prepare ownership was already decided: " + identity.ddlId().value());
            }
            prepareState = PrepareState.PREPARING_DURABLE;
            waitReason = OnlineDdlWaitReason.PREPARE_DURABILITY;
            touch();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * marker append确认成功后发布完整durable投影并唤醒等待取消者。
     *
     * @param record repository刚完成单batch append的同一DDL marker
     */
    public void markDurablePrepared(DdlLogRecord record) {
        lock.lock();
        try {
            if (prepareState != PrepareState.PREPARING_DURABLE) {
                throw new DatabaseValidationException(
                        "Online DDL marker completed without prepare ownership: " + identity.ddlId().value());
            }
            requireRecordIdentity(record);
            prepareState = PrepareState.DURABLE;
            applyDurable(record);
            waitReason = OnlineDdlWaitReason.NONE;
            touch();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * marker append结果不确定时永久关闭内存handoff；等待者只能收到异常并通过重启读取catalog裁决。
     *
     * @param errorCode 有界领域错误码，不得传入异常message或stack
     */
    public void failDurablePrepare(String errorCode) {
        validateErrorCode(errorCode);
        lock.lock();
        try {
            prepareState = PrepareState.FAILED_CLOSED;
            runtimePhase = OnlineDdlRuntimePhase.FAILED_CLOSED;
            terminalResult = OnlineDdlTerminalResult.FAILED_CLOSED;
            lastErrorCode = Optional.of(errorCode);
            waitReason = OnlineDdlWaitReason.NONE;
            touch();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * cancel线程参与prepare handoff：OPEN时原子接受内存取消；PREPARING时有界等待durable/failed结果。
     *
     * @param timeout 等待coordinator marker append完成的正上界
     * @return true表示取消在prepare前胜出；false表示marker已durable，调用方必须继续repository CAS
     * @throws OnlineDdlControlTimeoutException timeout或中断时抛出，不能返回accepted
     */
    boolean requestCancelBeforePrepare(Duration timeout) {
        validateTimeout(timeout);
        long remaining = boundedTimeoutNanos(timeout);
        lock.lock();
        try {
            if (prepareState == PrepareState.PREPARE_OPEN) {
                prepareState = PrepareState.CANCELLED_BEFORE_PREPARE;
                runtimePhase = OnlineDdlRuntimePhase.ABORTING;
                touch();
                changed.signalAll();
                return true;
            }
            while (prepareState == PrepareState.PREPARING_DURABLE) {
                if (remaining <= 0) {
                    throw new OnlineDdlControlTimeoutException(
                            "timed out waiting Online DDL prepare: " + identity.ddlId().value());
                }
                try {
                    remaining = changed.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new OnlineDdlControlTimeoutException(
                            "interrupted waiting Online DDL prepare: " + identity.ddlId().value(),
                            interrupted);
                }
            }
            if (prepareState == PrepareState.FAILED_CLOSED) {
                throw new OnlineDdlControlTimeoutException(
                        "Online DDL prepare outcome is failed-closed: " + identity.ddlId().value());
            }
            return prepareState == PrepareState.CANCELLED_BEFORE_PREPARE;
        } finally {
            lock.unlock();
        }
    }

    /** 用repository或恢复器观察到的完整marker刷新durable字段，不改变业务状态。 */
    public void observeDurable(DdlLogRecord record) {
        lock.lock();
        try {
            requireRecordIdentity(record);
            prepareState = PrepareState.DURABLE;
            applyDurable(record);
            touch();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** 更新runtime阶段/等待原因；terminal tracker禁止重新进入运行态。 */
    public void advanceRuntime(
            OnlineDdlRuntimePhase next, OnlineDdlWaitReason reason) {
        if (next == null || reason == null) {
            throw new DatabaseValidationException("Online DDL runtime phase/wait reason must not be null");
        }
        lock.lock();
        try {
            if (terminalResult != OnlineDdlTerminalResult.NONE) {
                throw new DatabaseValidationException(
                        "terminal Online DDL tracker cannot advance: " + identity.ddlId().value());
            }
            runtimePhase = next;
            waitReason = reason;
            touch();
        } finally {
            lock.unlock();
        }
    }

    /** 在一个短临界区更新gate快照，字段允许随排空增减但不能为负。 */
    public void updateGate(
            OnlineDdlTablePhase phase, long admissions, long leases, long redoHighWater) {
        if (phase == null || admissions < 0 || leases < 0 || redoHighWater < 0) {
            throw new DatabaseValidationException("invalid Online DDL gate projection");
        }
        lock.lock();
        try {
            gatePhase = phase;
            inFlightAdmissions = admissions;
            ioLeases = leases;
            terminalRedoHighWater = Math.max(terminalRedoHighWater, redoHighWater);
            touch();
        } finally {
            lock.unlock();
        }
    }

    /** 每个base-scan批次一次性增加行/批计数并替换安全key摘要。 */
    public void addScanBatch(long rows, Optional<String> clusteredKeyDigest) {
        if (rows < 0 || clusteredKeyDigest == null) {
            throw new DatabaseValidationException("invalid Online DDL scan progress");
        }
        clusteredKeyDigest.ifPresent(value -> validateBounded(value, 128, "clustered key digest"));
        lock.lock();
        try {
            rowsScanned = Math.addExact(rowsScanned, rows);
            batchesScanned = Math.addExact(batchesScanned, 1);
            lastClusteredKeyDigest = clusteredKeyDigest;
            touch();
        } finally {
            lock.unlock();
        }
    }

    /** 设置可选statistics估计；未知时传empty，不能用0伪装估计。 */
    public void updateEstimatedRows(OptionalLong estimate) {
        if (estimate == null || estimate.isPresent() && estimate.orElseThrow() < 0) {
            throw new DatabaseValidationException("invalid Online DDL row estimate");
        }
        lock.lock();
        try {
            estimatedRows = estimate;
            touch();
        } finally {
            lock.unlock();
        }
    }

    /** 一次性复制row-log标量投影；同generation内所有累计字段禁止倒退。 */
    public void updateChangeLog(
            long candidates, long sizeBytes, long maximumBytes, int reserveBytes,
            long appended, long forced, long generation) {
        if (candidates < 0 || sizeBytes < 0 || maximumBytes <= 0
                || reserveBytes < 0 || reserveBytes >= maximumBytes
                || appended < 0 || forced < 0 || forced > appended || generation <= 0) {
            throw new DatabaseValidationException("invalid Online DDL change-log projection");
        }
        lock.lock();
        try {
            if (candidates < candidateCount || generation < rowLogGeneration
                    || generation == rowLogGeneration
                    && (appended < highestAppendedSequence || forced < highestForcedSequence)) {
                throw new DatabaseValidationException("Online DDL change-log projection regressed");
            }
            candidateCount = candidates;
            rowLogBytes = sizeBytes;
            maxRowLogBytes = maximumBytes;
            abortReserveBytes = reserveBytes;
            highestAppendedSequence = appended;
            highestForcedSequence = forced;
            rowLogGeneration = generation;
            touch();
        } finally {
            lock.unlock();
        }
    }

    /** 更新retirement fence诊断投影；present/safe只能单调从false变true。 */
    public void updateRetirement(boolean present, boolean safe) {
        if (safe && !present) {
            throw new DatabaseValidationException("safe Online DDL retirement requires a fence");
        }
        lock.lock();
        try {
            if (retirementFencePresent && !present || retirementSafe && !safe) {
                throw new DatabaseValidationException("Online DDL retirement projection regressed");
            }
            retirementFencePresent = present;
            retirementSafe = safe;
            touch();
        } finally {
            lock.unlock();
        }
    }

    /**
     * registry在移入history前发布唯一terminal结果并唤醒awaitTerminal。
     *
     * @param result COMPLETED/ROLLED_BACK/FAILED_CLOSED之一
     * @param errorCode 可选有界领域错误码
     * @param forwardRecoveryRequired forward fence后仍需重启前滚时为true；pre-forward回滚失败仍保持false
     * @return terminal不可变快照
     */
    OnlineDdlOperationSnapshot complete(
            OnlineDdlTerminalResult result, Optional<String> errorCode,
            boolean forwardRecoveryRequired) {
        if (result == null || result == OnlineDdlTerminalResult.NONE || errorCode == null) {
            throw new DatabaseValidationException("invalid Online DDL terminal projection");
        }
        errorCode.ifPresent(OnlineDdlOperationTracker::validateErrorCode);
        lock.lock();
        try {
            if (terminalResult != OnlineDdlTerminalResult.NONE && terminalResult != result) {
                throw new DatabaseValidationException("Online DDL terminal result cannot change");
            }
            terminalResult = result;
            runtimePhase = switch (result) {
                case COMPLETED -> OnlineDdlRuntimePhase.COMPLETED;
                case ROLLED_BACK -> OnlineDdlRuntimePhase.ROLLED_BACK;
                case FAILED_CLOSED -> OnlineDdlRuntimePhase.FAILED_CLOSED;
                case NONE -> throw new DatabaseValidationException("NONE is not terminal");
            };
            lastErrorCode = errorCode;
            this.forwardRecoveryRequired = forwardRecoveryRequired;
            waitReason = OnlineDdlWaitReason.NONE;
            touch();
            changed.signalAll();
            return snapshotLocked();
        } finally {
            lock.unlock();
        }
    }

    /** 返回当前不可变弱一致投影；复制期间不访问任何下游协作者。 */
    public OnlineDdlOperationSnapshot snapshot() {
        lock.lock();
        try {
            return snapshotLocked();
        } finally {
            lock.unlock();
        }
    }

    /** 有界等待terminal；timeout返回empty，不把超时伪装成FAILED_CLOSED。 */
    Optional<OnlineDdlOperationSnapshot> awaitTerminal(Duration timeout) {
        validateTimeout(timeout);
        long remaining = boundedTimeoutNanos(timeout);
        lock.lock();
        try {
            while (terminalResult == OnlineDdlTerminalResult.NONE) {
                if (remaining <= 0) {
                    return Optional.empty();
                }
                try {
                    remaining = changed.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new OnlineDdlControlTimeoutException(
                            "interrupted waiting Online DDL terminal: " + identity.ddlId().value(),
                            interrupted);
                }
            }
            return Optional.of(snapshotLocked());
        } finally {
            lock.unlock();
        }
    }

    /** @return immutable operation identity。 */
    public OnlineDdlOperationIdentity identity() {
        return identity;
    }

    private OnlineDdlOperationSnapshot snapshotLocked() {
        return new OnlineDdlOperationSnapshot(
                epoch, identity, runtimePhase, gatePhase, startedAtEpochMillis,
                updatedAtEpochMillis, waitReason, durablePhase, controlState,
                rowLogGeneration, cancellationReason, rowsScanned, batchesScanned,
                estimatedRows, lastClusteredKeyDigest, candidateCount, rowLogBytes,
                maxRowLogBytes, abortReserveBytes, highestAppendedSequence,
                highestForcedSequence, inFlightAdmissions, ioLeases,
                terminalRedoHighWater, protocol.cancelCapable(), retirementFencePresent,
                retirementSafe, terminalResult, lastErrorCode, forwardRecoveryRequired);
    }

    private void requireRecordIdentity(DdlLogRecord record) {
        boolean secondaryIdentityMatches = record != null
                && (identity.operation() == DdlLogOperation.REBUILD_TABLE
                ? record.secondaryObjectId() > 0
                : record.secondaryObjectId() == identity.indexId());
        if (record == null || record.marker().ddlOperationId() != identity.ddlId().value()
                || record.marker().affectedObjectId() != identity.tableId()
                || !secondaryIdentityMatches
                || record.operation() != identity.operation()
                || record.executionProtocol() != protocol) {
            throw new DatabaseValidationException(
                    "Online DDL tracker/marker identity mismatch: " + identity.ddlId().value());
        }
    }

    private void applyDurable(DdlLogRecord record) {
        durablePhase = Optional.of(record.phase());
        controlState = record.controlState();
        cancellationReason = record.cancellation().map(DdlCancellation::reasonCode);
        retirementFencePresent = record.retirementFence().isPresent();
        forwardRecoveryRequired = record.controlState() == DdlControlState.FORWARD_ONLY
                && !record.phase().terminal();
    }

    private void touch() {
        epoch = Math.addExact(epoch, 1);
        updatedAtEpochMillis = Math.max(updatedAtEpochMillis, System.currentTimeMillis());
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("Online DDL control timeout must be positive");
        }
    }

    /** 把合法正Duration换成Condition可接受的纳秒预算；超出long时饱和而不是把控制请求误判为非法。 */
    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static void validateErrorCode(String value) {
        validateBounded(value, 64, "error code");
    }

    private static void validateBounded(String value, int limit, String field) {
        if (value == null || value.isBlank() || value.length() > limit) {
            throw new DatabaseValidationException(
                    "Online DDL " + field + " is blank or exceeds " + limit);
        }
    }
}
