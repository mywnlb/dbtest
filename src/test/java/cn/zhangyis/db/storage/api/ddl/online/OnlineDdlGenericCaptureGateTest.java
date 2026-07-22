package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 同一gate必须能承载非单索引的通用capture target，同时保留事务force high-water。 */
class OnlineDdlGenericCaptureGateTest {

    /** 通用target的candidate必须在commit force前保持capture id和sequence归属。 */
    @Test
    void capturesAndForcesGeneralAlterCandidate() {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        FakeChangeLog log = new FakeChangeLog(OnlineDdlCaptureId.of(71));
        OnlineDdlCaptureTarget target = new FakeTarget(log.captureId(), 72, log);
        TransactionId transactionId = TransactionId.of(73);

        gate.beginActivation(72, target.captureId(), Duration.ofSeconds(1));
        gate.publishCapture(target);
        try (OnlineDmlAdmission admission = gate.admit(
                transactionId, 72, Duration.ofSeconds(1))) {
            assertSame(target, admission.capture().orElseThrow());
            assertTrue(admission.captureTarget().isEmpty());
            assertEquals(1, admission.appendCandidate(new byte[]{1, 2, 3}));
        }

        assertEquals(1, gate.candidateHighWater(transactionId, target.captureId()));
        gate.forceTransactionCandidates(transactionId, Duration.ofSeconds(1));
        assertEquals(1, log.forcedSequence);
    }

    private record FakeTarget(OnlineDdlCaptureId captureId, long tableId,
                              OnlineDdlChangeLog changeLog)
            implements OnlineDdlCaptureTarget {
        @Override
        public OnlineDdlCandidateCodec candidateCodec() {
            return new OnlineDdlCandidateCodec() {
                @Override
                public Optional<byte[]> encodeInsert(LogicalRecord after) {
                    return Optional.of(new byte[]{1});
                }

                @Override
                public Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after) {
                    return Optional.of(new byte[]{2});
                }

                @Override
                public Optional<byte[]> encodeDelete(LogicalRecord before) {
                    return Optional.of(new byte[]{3});
                }
            };
        }
    }

    private static final class FakeChangeLog implements OnlineDdlChangeLog {
        private final OnlineDdlCaptureId captureId;
        private long appendedSequence;
        private long forcedSequence;

        private FakeChangeLog(OnlineDdlCaptureId captureId) {
            this.captureId = captureId;
        }

        @Override
        public OnlineDdlCaptureId captureId() {
            return captureId;
        }

        @Override
        public Path path() {
            return Path.of("fake-online-alter.log").toAbsolutePath();
        }

        @Override
        public long appendCandidate(TransactionId transactionId, byte[] payload) {
            return ++appendedSequence;
        }

        @Override
        public void forceThrough(long sequence, Duration timeout) {
            forcedSequence = sequence;
        }

        @Override
        public void markAbortRequired(OnlineDdlAbortReason reason, Duration timeout) {
        }

        @Override
        public boolean abortRequired() {
            return false;
        }

        @Override
        public void close() {
        }
    }
}
