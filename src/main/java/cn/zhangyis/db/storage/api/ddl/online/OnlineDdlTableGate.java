package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * StorageEngine 唯一拥有的 Online DDL table gate。短显式锁只保护 table phase、事务引用和 I/O lease 计数；
 * FileChannel、redo、B+Tree、page latch 与 DD 调用全部在锁外执行，避免形成 DDL 锁到存储慢 I/O 的反向边。
 */
public final class OnlineDdlTableGate {

    /** 保护全部运行期投影；等待期间 Condition 会释放该锁，不会串行化不相关表的实际 DML。 */
    private final ReentrantLock lock = new ReentrantLock(true);
    /** phase、in-flight、事务终态或 I/O lease 改变时统一唤醒，等待者必须循环复核自己的表谓词。 */
    private final Condition changed = lock.newCondition();
    /** table identity 到 gate state；entry 只由 {@link #lock} 保护和创建。 */
    private final Map<Long, TableState> tables = new HashMap<>();
    /** write transaction 到 affected tables/candidate high-water；直到 terminal 回调才删除。 */
    private final Map<TransactionId, TransactionState> transactions = new HashMap<>();

    /**
     * 为一次 clustered mutation 取得有界 admission，并始终登记 transaction/table 关系。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验正事务与表 identity，并在 timeout 内取得短 gate 锁。</li>
     *     <li>ACTIVATING/SEALING/SEALED 时循环有界等待；ABSENT、CAPTURING、RETIREMENT_OPEN、ABORTING 可进入。</li>
     *     <li>登记 affected table、递增 in-flight；仅 CAPTURING 冻结 target，DROP retirement不产生row-log。</li>
     * </ol>
     *
     * @param transactionId 已由 TransactionManager 分配的正 write id
     * @param tableId clustered command 的稳定正表 identity
     * @param timeout admission 等待 DDL freeze 解除的总上限，必须为正
     * @return 必须关闭的短 guard；CAPTURING 时携带不可替换 target
     * @throws DatabaseRuntimeException 超时或中断时抛出，失败不会递增 in-flight 或创建事务引用
     */
    public OnlineDmlAdmission admit(TransactionId transactionId, long tableId, Duration timeout) {
        validateTransactionAndTable(transactionId, tableId);
        long remaining = acquire(timeout, "admit table " + tableId);
        try {
            TableState table = table(tableId);
            while (blocksAdmission(table.phase)) {
                remaining = await(remaining, "online DML admission table " + tableId);
            }
            TransactionState transaction = transactions.computeIfAbsent(
                    transactionId, ignored -> new TransactionState());
            transaction.affectedTables.add(tableId);
            table.inFlightAdmissions++;
            OnlineDdlCaptureTarget target = table.phase == OnlineDdlTablePhase.CAPTURING
                    ? table.target : null;
            return new OnlineDmlAdmission(this, transactionId, tableId, target);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 进入 initial freeze 并等待旧 admission 与旧写事务排空；成功后保持 ACTIVATING，供 coordinator 在 X MDL
     * 下持久化 manifest、DDL marker 和 staged descriptor。
     *
     * @param tableId 目标正表 identity
     * @param buildId 新 DDL operation identity
     * @param timeout freeze 的总等待上限
     * @throws DatabaseRuntimeException 超时/中断时恢复 ABSENT 并唤醒 DML，调用方可安全回滚未发布 build
     */
    public void beginActivation(long tableId, OnlineIndexBuildId buildId, Duration timeout) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
        beginActivation(tableId, OnlineDdlCaptureId.of(buildId.value()), timeout);
    }

    /**
     * 通用Online ALTER进入initial freeze；与单索引入口共用同一table owner和事务排空规则。
     *
     * @param tableId 目标正表identity
     * @param captureId 与DDL marker相同的通用capture identity
     * @param timeout 取得gate并等待旧writer的正上限
     */
    public void beginActivation(long tableId, OnlineDdlCaptureId captureId, Duration timeout) {
        validateCaptureAndTable(captureId, tableId);
        long remaining = acquire(timeout, "activate table " + tableId);
        try {
            TableState table = table(tableId);
            if (table.phase != OnlineDdlTablePhase.ABSENT) {
                throw new DatabaseValidationException("online DDL table already has active build: " + tableId);
            }
            table.phase = OnlineDdlTablePhase.ACTIVATING;
            table.captureId = captureId;
            try {
                while (table.inFlightAdmissions != 0 || hasActiveTransaction(tableId)) {
                    remaining = await(remaining, "online DDL activation table " + tableId);
                }
            } catch (RuntimeException failure) {
                restoreAbsent(table);
                changed.signalAll();
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 为 Online DROP 声明单表 retirement owner，但不冻结 DML、不创建 capture target。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table/operation identity与等待预算，并有界取得gate短锁。</li>
     *     <li>要求表尚无其它online operation，随后原子发布build owner与RETIREMENT_OPEN。</li>
     *     <li>唤醒诊断等待者后返回；既有与后续DML继续按source metadata维护待删索引。</li>
     * </ol>
     *
     * @param tableId Online DROP目标的稳定正表 identity
     * @param buildId 与 DDL marker identity 相同的正运行期 operation id
     * @param timeout 取得 gate 短锁的正有界时限
     * @throws DatabaseValidationException identity无效或同表已有online operation时抛出
     * @throws DatabaseRuntimeException gate锁等待超时或中断时抛出，表状态保持ABSENT
     */
    public void beginRetirement(long tableId, OnlineIndexBuildId buildId, Duration timeout) {
        // 1. 与 ADD activation 共用identity/timeout校验和有界显式锁，不引入第二套并发所有权。
        validateBuildAndTable(buildId, tableId);
        acquire(timeout, "begin retirement table " + tableId);
        try {
            // 2. SU会排除其它DDL，gate仍做内存exact-owner校验，防止错误组合根绕过MDL。
            TableState table = table(tableId);
            if (table.phase != OnlineDdlTablePhase.ABSENT) {
                throw new DatabaseValidationException(
                        "online DDL table already has active operation: " + tableId);
            }
            table.captureId = OnlineDdlCaptureId.of(buildId.value());
            table.phase = OnlineDdlTablePhase.RETIREMENT_OPEN;

            // 3. RETIREMENT_OPEN不在blocksAdmission集合内，DML将登记事务引用但不会取得capture target。
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在 manifest/marker/descriptor 已 durable 后发布 CAPTURING target 并唤醒 DML。
     *
     * @param target 与 ACTIVATING build/table 和持久 row-log owner 完全一致的冻结目标
     * @throws DatabaseValidationException phase 或 identity 不匹配时抛出，原 ACTIVATING freeze 保持不变
     */
    public void publishCapture(OnlineDdlCaptureTarget target) {
        if (target == null) {
            throw new DatabaseValidationException("online capture target must not be null");
        }
        lock.lock();
        try {
            TableState table = table(target.tableId());
            if (table.phase != OnlineDdlTablePhase.ACTIVATING
                    || !target.captureId().equals(table.captureId)
                    || !target.changeLog().captureId().equals(target.captureId())) {
                throw new DatabaseValidationException("online capture target does not match activation");
            }
            table.target = target;
            table.phase = OnlineDdlTablePhase.CAPTURING;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 进入 final freeze，等待 admission、事务和 row-log lease 全部归零；成功后发布 SEALED。
     *
     * @param buildId 当前 CAPTURING build identity
     * @param timeout final MDL X 窗口内允许的总等待上限
     * @throws DatabaseRuntimeException 超时/中断时恢复 CAPTURING，DML 重新获准进入
     */
    public void beginSeal(OnlineIndexBuildId buildId, Duration timeout) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
        beginSeal(OnlineDdlCaptureId.of(buildId.value()), timeout);
    }

    /** 通用capture进入final seal；超时仍遵循调用协议既有的pre-forward恢复语义。 */
    public void beginSeal(OnlineDdlCaptureId captureId, Duration timeout) {
        requireCapture(captureId);
        long remaining = acquire(timeout, "seal capture " + captureId.value());
        try {
            TableState table = requireTableByCapture(captureId);
            OnlineDdlTablePhase openPhase = table.phase;
            if (openPhase != OnlineDdlTablePhase.CAPTURING
                    && openPhase != OnlineDdlTablePhase.RETIREMENT_OPEN) {
                throw new DatabaseValidationException(
                        "online DDL operation is not open for finalization: " + captureId.value());
            }
            table.phase = OnlineDdlTablePhase.SEALING;
            try {
                while (table.inFlightAdmissions != 0 || table.ioLeases != 0
                        || hasActiveTransaction(table.tableId)) {
                    remaining = await(remaining, "online DDL seal capture " + captureId.value());
                    if (table.phase == OnlineDdlTablePhase.ABORTING) {
                        throw new DatabaseRuntimeException(
                                "online DDL seal observed durable abort: " + captureId.value());
                    }
                }
                if (table.phase != OnlineDdlTablePhase.SEALING) {
                    throw new DatabaseRuntimeException(
                            "online DDL seal lost SEALING ownership: " + captureId.value()
                                    + " phase=" + table.phase);
                }
                table.phase = OnlineDdlTablePhase.SEALED;
            } catch (RuntimeException failure) {
                if (table.phase == OnlineDdlTablePhase.SEALING) {
                    // timeout仍在forward fence前，恢复原operation语义：ADD继续capture，DROP继续维护source index。
                    table.phase = openPhase;
                }
                changed.signalAll();
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * shadow final barrier在尚未写日志SEALED/FORWARD_ONLY时失败，将gate从SEALED恢复CAPTURING。
     * 调用方必须仍持final table X，确保恢复与新DML admission之间不存在未受控空窗。
     *
     * @param captureId 当前SEALED通用capture owner
     * @throws DatabaseValidationException owner或phase不匹配时抛出并保持原状态
     */
    public void resumeCapture(OnlineDdlCaptureId captureId) {
        requireCapture(captureId);
        lock.lock();
        try {
            TableState table = requireTableByCapture(captureId);
            if (table.phase != OnlineDdlTablePhase.SEALED || table.target == null) {
                throw new DatabaseValidationException(
                        "online DDL resume requires SEALED capture: " + captureId.value());
            }
            table.phase = OnlineDdlTablePhase.CAPTURING;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 把 build 切入 ABORTING，作废所有事务的 candidate force requirement 并允许新 DML 无捕获进入。
     *
     * @param buildId 待终止 build identity
     * @param reason 已先由 row log durable 记录或由 coordinator 持有的终止原因
     */
    public void beginAbort(OnlineIndexBuildId buildId, OnlineDdlAbortReason reason) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
        beginAbort(OnlineDdlCaptureId.of(buildId.value()), reason);
    }

    /** 把通用capture切入ABORTING并作废所有事务force requirement。 */
    public void beginAbort(OnlineDdlCaptureId captureId, OnlineDdlAbortReason reason) {
        requireCapture(captureId);
        if (reason == null) {
            throw new DatabaseValidationException("online DDL abort reason must not be null");
        }
        lock.lock();
        try {
            TableState table = requireTableByCapture(captureId);
            table.phase = OnlineDdlTablePhase.ABORTING;
            for (TransactionState transaction : transactions.values()) {
                transaction.candidates.remove(captureId);
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * ABORTING 后有界等待已经在 gate 锁外执行的 append/force lease 退出。新 admission 已拿不到 capture target，
     * 因而本方法返回后 coordinator 可以关闭 row-log 和回收 staged tree，不会与迟到文件 I/O 竞态。
     *
     * @param buildId 已切入 ABORTING 的 build identity
     * @param timeout 等待既有 row-log I/O lease 清零的总上限
     * @throws DatabaseRuntimeException phase 不匹配、超时或中断时抛出，build 保持 ABORTING 供重试/恢复
     */
    public void awaitAbortQuiescence(OnlineIndexBuildId buildId, Duration timeout) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
        awaitAbortQuiescence(OnlineDdlCaptureId.of(buildId.value()), timeout);
    }

    /** 等待通用capture的锁外append/force lease全部退出。 */
    public void awaitAbortQuiescence(OnlineDdlCaptureId captureId, Duration timeout) {
        requireCapture(captureId);
        long remaining = acquire(timeout, "wait abort capture " + captureId.value());
        try {
            TableState table = requireTableByCapture(captureId);
            if (table.phase != OnlineDdlTablePhase.ABORTING) {
                throw new DatabaseValidationException(
                        "online DDL abort drain requires ABORTING phase: " + captureId.value());
            }
            while (table.ioLeases != 0) {
                remaining = await(remaining,
                        "online DDL abort I/O drain capture " + captureId.value());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * terminal cleanup 后移除 build 的运行期 target；事务 affected-table 投影继续存在，供后续 build freeze 使用。
     *
     * @param buildId 已 SEALED 或 ABORTING 且物理/文件 cleanup 完成的 identity
     */
    public void clearBuild(OnlineIndexBuildId buildId) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
        clearCapture(OnlineDdlCaptureId.of(buildId.value()));
    }

    /** terminal cleanup后移除通用capture运行期owner。 */
    public void clearCapture(OnlineDdlCaptureId captureId) {
        requireCapture(captureId);
        lock.lock();
        try {
            TableState table = requireTableByCapture(captureId);
            if (table.ioLeases != 0
                    || (table.phase != OnlineDdlTablePhase.ABORTING
                    && table.inFlightAdmissions != 0)) {
                throw new DatabaseRuntimeException(
                        "online DDL capture still owns gate leases: " + captureId.value());
            }
            restoreAbsent(table);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在 undo terminal MTR 之前按 build id 顺序 force 本事务全部 candidate high-water。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在 gate 锁内复制仍有效的 build/sequence requirement，并给相应表增加 I/O lease。</li>
     *     <li>释放 gate 锁，按 build id 顺序调用 row-log force，避免与 DDL seal 或其它表状态互锁。</li>
     *     <li>finally 重新取得 gate 锁释放全部 lease；失败保持事务 ACTIVE，调用方可以走 rollback。</li>
     * </ol>
     *
     * @param transactionId 即将 COMMIT 或 XA PREPARE 的正 write id
     * @param timeout 每个 row-log force 的正上限
     * @throws DatabaseRuntimeException 文件 force 失败时抛出；不会移除 transaction/table 引用
     */
    public void forceTransactionCandidates(TransactionId transactionId, Duration timeout) {
        validateTransaction(transactionId);
        validateTimeout(timeout);
        List<ForceRequirement> requirements = new ArrayList<>();
        lock.lock();
        try {
            TransactionState transaction = transactions.get(transactionId);
            if (transaction == null) {
                return;
            }
            transaction.candidates.values().stream()
                    .sorted(Comparator.comparing(requirement -> requirement.target.captureId()))
                    .forEach(requirement -> {
                        TableState table = tables.get(requirement.target.tableId());
                        if (table != null && table.target == requirement.target
                                && table.phase != OnlineDdlTablePhase.ABORTING) {
                            table.ioLeases++;
                            requirements.add(requirement);
                        }
                    });
        } finally {
            lock.unlock();
        }

        try {
            for (ForceRequirement requirement : requirements) {
                requirement.target.changeLog().forceThrough(requirement.sequence, timeout);
            }
        } finally {
            lock.lock();
            try {
                for (ForceRequirement requirement : requirements) {
                    TableState table = tables.get(requirement.target.tableId());
                    if (table != null) {
                        table.ioLeases--;
                    }
                }
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * COMMITTED/phase-two COMMIT 已发布 terminal redo 后释放事务引用并推进每张表的 redo high-water。
     *
     * @param transactionId 已进入 COMMITTED 的正 write id
     * @param terminalRedoLsn 覆盖 undo terminal 状态的 redo LSN；final cutover 必须等待其 durable
     */
    public void completeCommit(TransactionId transactionId, Lsn terminalRedoLsn) {
        validateTransaction(transactionId);
        if (terminalRedoLsn == null) {
            throw new DatabaseValidationException("online DDL terminal redo LSN must not be null");
        }
        lock.lock();
        try {
            TransactionState transaction = transactions.remove(transactionId);
            if (transaction == null) {
                return;
            }
            for (long tableId : transaction.affectedTables) {
                TableState table = table(tableId);
                if (terminalRedoLsn.value() > table.terminalRedoHighWater.value()) {
                    table.terminalRedoHighWater = terminalRedoLsn;
                }
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * ROLLED_BACK/phase-two ROLLBACK 已 durable 后释放事务引用；rollback 不推进 committed redo high-water。
     *
     * @param transactionId 已进入完整回滚终态的正 write id
     */
    public void completeRollback(TransactionId transactionId) {
        validateTransaction(transactionId);
        lock.lock();
        try {
            if (transactions.remove(transactionId) != null) {
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** @return 指定表的当前运行期 phase；从未出现的表返回 ABSENT。 */
    public OnlineDdlTablePhase phase(long tableId) {
        validateTable(tableId);
        lock.lock();
        try {
            TableState table = tables.get(tableId);
            return table == null ? OnlineDdlTablePhase.ABSENT : table.phase;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在单次 gate 短锁内复制指定表的 phase、build、lease、事务引用与 redo 高水位。
     *
     * @param tableId 诊断端请求的正 table identity；从未进入 gate 时仍返回 ABSENT 快照
     * @return 不包含可变 target/transaction 集合的不可变诊断投影
     */
    public OnlineDdlGateSnapshot snapshot(long tableId) {
        validateTable(tableId);
        lock.lock();
        try {
            TableState table = tables.get(tableId);
            if (table == null) {
                return new OnlineDdlGateSnapshot(tableId, java.util.Optional.empty(),
                        OnlineDdlTablePhase.ABSENT, 0, 0, 0, Lsn.of(0));
            }
            long activeTransactions = transactions.values().stream()
                    .filter(transaction -> transaction.affectedTables.contains(tableId))
                    .count();
            return new OnlineDdlGateSnapshot(tableId,
                    java.util.Optional.ofNullable(table.captureId)
                            .map(value -> OnlineIndexBuildId.of(value.value())), table.phase,
                    table.inFlightAdmissions, table.ioLeases, activeTransactions,
                    table.terminalRedoHighWater);
        } finally {
            lock.unlock();
        }
    }

    /** @return 指定事务/build 尚需 force 的 sequence；不存在、已终态或已 abort 返回 0。 */
    public long candidateHighWater(TransactionId transactionId, OnlineIndexBuildId buildId) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
        return candidateHighWater(transactionId, OnlineDdlCaptureId.of(buildId.value()));
    }

    /** @return 指定事务/capture尚需force的sequence；不存在、终态或abort返回0。 */
    public long candidateHighWater(TransactionId transactionId, OnlineDdlCaptureId captureId) {
        validateTransaction(transactionId);
        requireCapture(captureId);
        lock.lock();
        try {
            TransactionState transaction = transactions.get(transactionId);
            ForceRequirement requirement = transaction == null ? null
                    : transaction.candidates.get(captureId);
            return requirement == null ? 0 : requirement.sequence;
        } finally {
            lock.unlock();
        }
    }

    /** @return 已终态 committed 写事务在指定表上的最大 redo LSN；尚无提交时返回 LSN(0)。 */
    public Lsn terminalRedoHighWater(long tableId) {
        validateTable(tableId);
        lock.lock();
        try {
            TableState table = tables.get(tableId);
            return table == null ? Lsn.of(0) : table.terminalRedoHighWater;
        } finally {
            lock.unlock();
        }
    }

    long appendCandidate(TransactionId transactionId, long tableId,
                         OnlineDdlCaptureTarget target, byte[] payload) {
        lock.lock();
        try {
            TableState table = table(tableId);
            if (table.phase != OnlineDdlTablePhase.CAPTURING || table.target != target) {
                return 0;
            }
            table.ioLeases++;
        } finally {
            lock.unlock();
        }

        long sequence = 0;
        try {
            sequence = target.changeLog().appendCandidate(transactionId, payload);
            return sequence;
        } finally {
            lock.lock();
            try {
                TableState table = table(tableId);
                table.ioLeases--;
                // admission 在 CAPTURING 冻结 target 后，append 可能与 final seal 交叠并在 SEALING 才返回。
                // seal 会等待该事务终态，因此仍须登记 high-water，让 COMMIT/XA PREPARE 自己完成 candidate force；
                // 只有 ABORTING 会作废 requirement，不能把 SEALING 误当作丢弃证据的终态。
                boolean forceBeforeTerminal = table.phase == OnlineDdlTablePhase.CAPTURING
                        || table.phase == OnlineDdlTablePhase.SEALING;
                if (sequence > 0 && forceBeforeTerminal && table.target == target) {
                    TransactionState transaction = transactions.get(transactionId);
                    if (transaction != null) {
                        ForceRequirement previous = transaction.candidates.get(target.captureId());
                        if (previous == null || sequence > previous.sequence) {
                            transaction.candidates.put(target.captureId(),
                                    new ForceRequirement(target, sequence));
                        }
                    }
                } else if (sequence == 0 && target.changeLog().abortRequired()
                        && table.target == target) {
                    table.phase = OnlineDdlTablePhase.ABORTING;
                    for (TransactionState transaction : transactions.values()) {
                        transaction.candidates.remove(target.captureId());
                    }
                }
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    void closeAdmission(long tableId) {
        lock.lock();
        try {
            TableState table = table(tableId);
            if (table.inFlightAdmissions <= 0) {
                throw new DatabaseRuntimeException("online DDL admission count underflow for table " + tableId);
            }
            table.inFlightAdmissions--;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private long acquire(Duration timeout, String action) {
        validateTimeout(timeout);
        long nanos = boundedTimeoutNanos(timeout);
        long started = System.nanoTime();
        try {
            if (!lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
                throw new DatabaseRuntimeException(action + " timed out acquiring online DDL gate");
            }
            long elapsed = System.nanoTime() - started;
            return elapsed <= 0L ? nanos : elapsed >= nanos ? 0L : nanos - elapsed;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException(action + " interrupted acquiring online DDL gate", interrupted);
        }
    }

    /** 把正Duration换成Condition/tryLock可接受的纳秒预算；表示溢出时使用最大有界等待。 */
    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private long await(long remainingNanos, String action) {
        if (remainingNanos <= 0) {
            throw new DatabaseRuntimeException(action + " timed out");
        }
        try {
            long next = changed.awaitNanos(remainingNanos);
            if (next <= 0) {
                throw new DatabaseRuntimeException(action + " timed out");
            }
            return next;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException(action + " interrupted", interrupted);
        }
    }

    private boolean hasActiveTransaction(long tableId) {
        return transactions.values().stream()
                .anyMatch(transaction -> transaction.affectedTables.contains(tableId));
    }

    private TableState table(long tableId) {
        return tables.computeIfAbsent(tableId, TableState::new);
    }

    private TableState requireTableByCapture(OnlineDdlCaptureId captureId) {
        return tables.values().stream()
                .filter(table -> captureId.equals(table.captureId))
                .findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "online DDL capture is not registered: " + captureId.value()));
    }

    private static boolean blocksAdmission(OnlineDdlTablePhase phase) {
        return phase == OnlineDdlTablePhase.ACTIVATING
                || phase == OnlineDdlTablePhase.SEALING
                || phase == OnlineDdlTablePhase.SEALED;
    }

    private static void restoreAbsent(TableState table) {
        table.phase = OnlineDdlTablePhase.ABSENT;
        table.captureId = null;
        table.target = null;
    }

    private static void validateTransactionAndTable(TransactionId transactionId, long tableId) {
        validateTransaction(transactionId);
        validateTable(tableId);
    }

    private static void validateBuildAndTable(OnlineIndexBuildId buildId, long tableId) {
        requireBuild(buildId);
        validateTable(tableId);
    }

    private static void validateCaptureAndTable(OnlineDdlCaptureId captureId, long tableId) {
        requireCapture(captureId);
        validateTable(tableId);
    }

    private static void validateTransaction(TransactionId transactionId) {
        if (transactionId == null || transactionId.isNone()) {
            throw new DatabaseValidationException("online DDL gate requires positive transaction id");
        }
    }

    private static void validateTable(long tableId) {
        if (tableId <= 0) {
            throw new DatabaseValidationException("online DDL gate requires positive table id");
        }
    }

    private static void requireBuild(OnlineIndexBuildId buildId) {
        if (buildId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive build id");
        }
    }

    private static void requireCapture(OnlineDdlCaptureId captureId) {
        if (captureId == null) {
            throw new DatabaseValidationException("online DDL gate requires positive capture id");
        }
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("online DDL gate timeout must be positive");
        }
    }

    /** 单表投影；所有字段由外层 gate lock 保护，不能把本对象泄露给调用方。 */
    private static final class TableState {
        private final long tableId;
        private OnlineDdlTablePhase phase = OnlineDdlTablePhase.ABSENT;
        private OnlineDdlCaptureId captureId;
        private OnlineDdlCaptureTarget target;
        private int inFlightAdmissions;
        private int ioLeases;
        private Lsn terminalRedoHighWater = Lsn.of(0);

        private TableState(long tableId) {
            this.tableId = tableId;
        }
    }

    /** 单事务投影；affected table 生命周期长于 admission，candidate requirement 在 abort 时可独立作废。 */
    private static final class TransactionState {
        private final Set<Long> affectedTables = new LinkedHashSet<>();
        private final Map<OnlineDdlCaptureId, ForceRequirement> candidates = new HashMap<>();
    }

    /** row-log force 的锁外不可变快照。 */
    private record ForceRequirement(OnlineDdlCaptureTarget target, long sequence) {
    }
}
