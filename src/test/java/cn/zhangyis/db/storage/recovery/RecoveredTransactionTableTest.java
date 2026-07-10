package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 正式事务恢复表测试：它只在恢复线程内按 redo 顺序积累证据，任何不能唯一解释的终态都必须阻止启动。
 */
class RecoveredTransactionTableTest {

    /** 初始 redo checkpoint 可以没有 sidecar；两个 next-counter 从 1 开始。 */
    @Test
    void zeroCheckpointWithoutSidecarUsesInitialCounters() {
        RecoveredTransactionSnapshot snapshot =
                RecoveredTransactionTable.open(Lsn.of(0), Optional.empty()).snapshot();

        assertEquals(TransactionId.of(1), snapshot.nextTransactionId());
        assertEquals(TransactionNo.of(1), snapshot.nextTransactionNo());
        assertEquals(0, snapshot.entries().size());
    }

    /** 非零 redo checkpoint 缺 sidecar 时无法证明被回收的纯 INSERT 事务 id，必须 fail closed。 */
    @Test
    void nonZeroCheckpointWithoutSidecarIsFatal() {
        assertThrows(TransactionRecoveryException.class,
                () -> RecoveredTransactionTable.open(Lsn.of(10), Optional.empty()));
    }

    /** sidecar 可以比 redo label 更新，但绝不能落后于 redo 恢复起点。 */
    @Test
    void sidecarMustCoverRedoCheckpoint() {
        assertThrows(TransactionRecoveryException.class,
                () -> RecoveredTransactionTable.open(Lsn.of(10), Optional.of(checkpoint(9, 7, 4))));

        RecoveredTransactionSnapshot snapshot = RecoveredTransactionTable
                .open(Lsn.of(10), Optional.of(checkpoint(12, 7, 4))).snapshot();
        assertEquals(TransactionId.of(7), snapshot.nextTransactionId());
        assertEquals(TransactionNo.of(4), snapshot.nextTransactionNo());
    }

    /** commit delta 进入不可变 snapshot，并把两个 counter 至少推进到证据值加一。 */
    @Test
    void committedDeltaBuildsTerminalEvidenceAndAdvancesCounters() {
        RecoveredTransactionTable table = table();
        TransactionStateDeltaRecord record = committed(7, 3);

        table.accept(range(100, 120), record);
        RecoveredTransactionSnapshot snapshot = table.snapshot();

        RecoveredTransactionEntry entry = snapshot.entry(TransactionId.of(7)).orElseThrow();
        assertEquals(RecoveredTransactionState.COMMITTED, entry.state());
        assertEquals(TransactionNo.of(3), entry.transactionNo());
        assertEquals(TransactionId.of(8), snapshot.nextTransactionId());
        assertEquals(TransactionNo.of(4), snapshot.nextTransactionNo());
    }

    /** 同一个 terminal record 在恢复重扫时是幂等的。 */
    @Test
    void exactDuplicateTerminalDeltaIsIdempotent() {
        RecoveredTransactionTable table = table();
        TransactionStateDeltaRecord record = committed(7, 3);

        table.accept(range(100, 120), record);
        table.accept(range(120, 140), record);

        assertEquals(1, table.snapshot().entries().size());
        assertEquals(Lsn.of(140), table.snapshot().entry(TransactionId.of(7)).orElseThrow().evidenceEndLsn());
    }

    /** 同一事务出现不同 commitNo 的 COMMITTED 证据不能靠 last-write-wins 掩盖。 */
    @Test
    void conflictingCommitNumberIsFatal() {
        RecoveredTransactionTable table = table();
        table.accept(range(100, 120), committed(7, 3));

        assertThrows(TransactionRecoveryException.class,
                () -> table.accept(range(120, 140), committed(7, 4)));
    }

    /** 同一事务出现 COMMITTED 与 ROLLED_BACK 两种终态时必须 fail closed。 */
    @Test
    void conflictingTerminalStateIsFatal() {
        RecoveredTransactionTable table = table();
        table.accept(range(100, 120), committed(7, 3));
        TransactionStateDeltaRecord rolledBack = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ROLLING_BACK,
                TransactionStateDeltaState.ROLLED_BACK, TransactionNo.NONE,
                TransactionStateDeltaReason.ROLLBACK);

        assertThrows(TransactionRecoveryException.class,
                () -> table.accept(range(120, 140), rolledBack));
    }

    /** PREPARED 有稳定磁盘 code，但 v1 没有 XA 决议器，不能猜测 commit/rollback。 */
    @Test
    void preparedEvidenceIsFatalWithoutXaCoordinator() {
        RecoveredTransactionTable table = table();
        TransactionStateDeltaRecord prepared = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.PREPARED, TransactionNo.NONE,
                TransactionStateDeltaReason.COMMIT);

        assertThrows(TransactionRecoveryException.class,
                () -> table.accept(range(100, 120), prepared));
    }

    /** CRC 合法不等于状态机合法；v1 只接受实际 producer 会写出的 terminal tuple。 */
    @Test
    void impossibleTerminalTransitionIsFatal() {
        RecoveredTransactionTable table = table();
        TransactionStateDeltaRecord impossibleCommit = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ROLLING_BACK,
                TransactionStateDeltaState.COMMITTED, TransactionNo.of(3),
                TransactionStateDeltaReason.COMMIT);

        assertThrows(TransactionRecoveryException.class,
                () -> table.accept(range(100, 120), impossibleCommit));
    }

    /** terminal 不能重新回到 ACTIVE；即使它是该事务看到的第一条 post-checkpoint record 也必须拒绝。 */
    @Test
    void transitionFromTerminalStateIsFatal() {
        RecoveredTransactionTable table = table();
        TransactionStateDeltaRecord terminalToActive = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.COMMITTED,
                TransactionStateDeltaState.ACTIVE, TransactionNo.of(3),
                TransactionStateDeltaReason.COMMIT);

        assertThrows(TransactionRecoveryException.class,
                () -> table.accept(range(100, 120), terminalToActive));
    }

    /** rollback 只保留 transaction-id 终态；预留但未提交的 transactionNo 不能伪装成提交高水位证据。 */
    @Test
    void liveRollbackCarryingCommitNumberIsFatal() {
        RecoveredTransactionTable table = table();
        TransactionStateDeltaRecord invalidRollback = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ROLLING_BACK,
                TransactionStateDeltaState.ROLLED_BACK, TransactionNo.of(3),
                TransactionStateDeltaReason.ROLLBACK);

        assertThrows(TransactionRecoveryException.class,
                () -> table.accept(range(100, 120), invalidRollback));
    }

    private static RecoveredTransactionTable table() {
        return RecoveredTransactionTable.open(Lsn.of(0), Optional.empty());
    }

    private static TransactionRecoveryCheckpoint checkpoint(long lsn, long nextId, long nextNo) {
        return new TransactionRecoveryCheckpoint(
                Lsn.of(lsn), TransactionId.of(nextId), TransactionNo.of(nextNo));
    }

    private static TransactionStateDeltaRecord committed(long transactionId, long transactionNo) {
        return new TransactionStateDeltaRecord(
                TransactionId.of(transactionId), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.COMMITTED, TransactionNo.of(transactionNo),
                TransactionStateDeltaReason.COMMIT);
    }

    private static LogRange range(long start, long end) {
        return new LogRange(Lsn.of(start), Lsn.of(end));
    }
}
