package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * redo capacity 前台 throttle 测试。该协作者只在 redo append/reservation 之前做压力判断、触发 flush/checkpoint，
 * 不直接持有 Buffer Pool page latch 或 redo 文件锁，避免容量等待扩大为存储页锁等待链。
 */
class RedoCapacityThrottleTest {

    @Test
    void asyncPressureRequestsFlushWithoutBlockingForeground() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(60, 0);
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce,
                Duration.ofMillis(1));

        throttle.beforeAppend();

        assertEquals(1, env.asyncRequests());
        assertEquals(0, env.flushRequests());
        assertEquals(0, env.checkpointAdvances());
    }

    @Test
    void syncPressureWaitsUntilCheckpointAdvancesBelowSyncThreshold() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(80, 0);
        env.onFlushCheckpointSequence(Lsn.of(0), Lsn.of(10), Lsn.of(40));
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce,
                Duration.ofMillis(50));

        throttle.beforeAppend();

        assertEquals(1, env.flushRequests());
        assertEquals(1, env.checkpointAdvances());
        assertEquals(Lsn.of(10), env.checkpointLsn());
    }

    @Test
    void reservationUsesProspectiveAppendBytesWhenEvaluatingPressure() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(70, 0);
        env.onFlushCheckpointSequence(Lsn.of(0), Lsn.of(10));
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce,
                Duration.ofMillis(50));

        try (RedoCapacityThrottle.Reservation ignored = throttle.reserveAppendBytes(10)) {
            assertEquals(1, env.flushRequests(),
                    "current age 70 is below sync, but reserving 10 bytes makes the prospective age 80");
            assertEquals(Lsn.of(10), env.checkpointLsn());
        }
    }

    @Test
    void physicalBudgetLargerThanOneRepositoryBatchFailsBeforeFlush() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(0, 0);
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100_000), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce, Duration.ofMillis(1), 512);
        RedoAppendBudget twoBlocks = RedoAppendBudget.upperBound(
                RedoBudgetPurpose.CLUSTERED_INSERT, 441);

        assertThrows(RedoBudgetTooLargeException.class,
                () -> throttle.reserveAppendBudget(twoBlocks));
        assertEquals(0, env.flushRequests());
        assertEquals(0, env.asyncRequests());
    }

    @Test
    void ownershipTransferAndCloseReleaseReservationExactlyOnce() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(50, 0);
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce, Duration.ZERO);
        RedoCapacityThrottle.Reservation first = throttle.reserveAppendBudget(
                RedoAppendBudget.upperBound(RedoBudgetPurpose.CLUSTERED_INSERT, 10));
        first.transferToAppend();
        first.close();

        try (RedoCapacityThrottle.Reservation ignored = throttle.reserveAppendBudget(
                RedoAppendBudget.upperBound(RedoBudgetPurpose.CLUSTERED_UPDATE, 9))) {
            assertEquals(0, env.flushRequests(),
                    "transferred budget must stop double-counting once real current LSN owns the append");
        }
    }

    @Test
    void hardPressureReleasesAfterFlushProgress() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(95, 0);
        env.onFlushCheckpointSequence(Lsn.of(0), Lsn.of(30));
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce,
                Duration.ofMillis(50));

        throttle.beforeAppend();

        assertEquals(1, env.flushRequests());
        assertEquals(Lsn.of(30), env.checkpointLsn(),
                "HARD_LIMIT waiter should be released once checkpoint age falls below SYNC_FLUSH");
    }

    @Test
    void hardPressureTimesOutFailClosedWhenCheckpointCannotAdvance() {
        FakeCapacityEnvironment env = new FakeCapacityEnvironment(95, 0);
        RedoCapacityThrottle throttle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(100), env::currentLsn, env::checkpointLsn,
                env::requestAsyncFlush, env::flushOnce,
                Duration.ZERO);

        RedoCapacityThrottleTimeoutException error = assertThrows(
                RedoCapacityThrottleTimeoutException.class, throttle::beforeAppend);

        assertTrue(error.getMessage().startsWith(
                "redo capacity throttle timed out at pressure HARD_LIMIT: current=95, checkpoint=0"),
                "timeout diagnostics should keep current/checkpoint and may append reservation details");
        assertEquals(1, env.flushRequests());
        assertEquals(0, env.checkpointAdvances());
    }

    private static final class FakeCapacityEnvironment {

        private final Lsn currentLsn;
        private final List<Lsn> checkpointSequence = new ArrayList<>();
        private int checkpointIndex;
        private int flushRequests;
        private int asyncRequests;
        private int checkpointAdvances;

        private FakeCapacityEnvironment(long currentLsn, long checkpointLsn) {
            this.currentLsn = Lsn.of(currentLsn);
            this.checkpointSequence.add(Lsn.of(checkpointLsn));
        }

        private Lsn currentLsn() {
            return currentLsn;
        }

        private Lsn checkpointLsn() {
            return checkpointSequence.get(checkpointIndex);
        }

        private void onFlushCheckpointSequence(Lsn... checkpoints) {
            checkpointSequence.clear();
            for (Lsn checkpoint : checkpoints) {
                checkpointSequence.add(checkpoint);
            }
            checkpointIndex = 0;
        }

        private void requestAsyncFlush() {
            asyncRequests++;
        }

        private void flushOnce() {
            flushRequests++;
            if (checkpointIndex + 1 < checkpointSequence.size()) {
                checkpointIndex++;
                checkpointAdvances++;
            }
        }

        private int flushRequests() {
            return flushRequests;
        }

        private int asyncRequests() {
            return asyncRequests;
        }

        private int checkpointAdvances() {
            return checkpointAdvances;
        }
    }
}
