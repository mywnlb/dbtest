package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 通用Online ALTER journal的持久状态机、容量终止与恢复扫描测试。 */
class FileOnlineAlterChangeLogTest {

    @TempDir
    Path directory;

    /** force覆盖candidate后重开必须恢复owner、序列、READY/RECONCILED发布证据。 */
    @Test
    void persistsCompletePublishStateMachineAndReopens() {
        Path path = path();
        long candidate;
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            startCapture(log);
            candidate = log.appendCandidate(TransactionId.of(91), new byte[]{4, 2});
            log.forceThrough(candidate, Duration.ofSeconds(2));
            log.appendState(OnlineAlterLogRecordType.SEALED, new byte[0]);
            log.appendState(OnlineAlterLogRecordType.READY_TO_PUBLISH, new byte[0]);
            long reconciled = log.appendState(OnlineAlterLogRecordType.RECONCILED, new byte[0]);
            log.forceThrough(reconciled, Duration.ofSeconds(2));
        }

        try (FileOnlineAlterChangeLog reopened = FileOnlineAlterChangeLog.open(
                path, 64 * 1024, 4096)) {
            assertEquals(header().captureId(), reopened.header().captureId());
            assertTrue(reopened.highestForcedSequence() >= candidate);
            assertTrue(reopened.snapshot().reconciled());
            assertEquals(OnlineAlterLogRecordType.RECONCILED,
                    reopened.readAll().stream()
                            .filter(record -> record.type() == OnlineAlterLogRecordType.RECONCILED)
                            .findFirst().orElseThrow().type());
        }
    }

    /** READY_TO_PUBLISH必须位于SEALED之后且RECONCILED之前，格式有效也不能绕过状态机。 */
    @Test
    void rejectsReadyBeforeSeal() {
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                path(), header(), 64 * 1024, 4096)) {
            startCapture(log);

            assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                    () -> log.appendState(OnlineAlterLogRecordType.READY_TO_PUBLISH, new byte[0]));
        }
    }

    /** candidate不得消费terminal reserve；容量不足须留下已force的ABORT_REQUIRED证据。 */
    @Test
    void reservesCapacityForDurableAbort() {
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                path(), header(), 8192, 4096)) {
            startCapture(log);

            assertEquals(0, log.appendCandidate(TransactionId.of(92), new byte[5000]));
            assertTrue(log.abortRequired());
            assertTrue(log.highestForcedSequence() > 0);
        }
    }

    /** torn tail可以截断；完整CRC损坏必须fail-closed，不能把中间证据当作未完成尾部丢弃。 */
    @Test
    void truncatesTornTailButRejectsCorruptedCompleteFrame() throws Exception {
        Path tornPath = path();
        long stableSize;
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                tornPath, header(), 64 * 1024, 4096)) {
            startCapture(log);
            long sequence = log.appendCandidate(TransactionId.of(93), new byte[]{1});
            log.forceThrough(sequence, Duration.ofSeconds(2));
            stableSize = Files.size(tornPath);
        }
        Files.write(tornPath, new byte[]{0, 0, 0, 80, 1},
                java.nio.file.StandardOpenOption.APPEND);
        try (FileOnlineAlterChangeLog ignored = FileOnlineAlterChangeLog.open(
                tornPath, 64 * 1024, 4096)) {
            assertEquals(stableSize, Files.size(tornPath));
        }

        Path corruptPath = directory.resolve("corrupt.log");
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                corruptPath, header(), 64 * 1024, 4096)) {
            startCapture(log);
        }
        byte[] bytes = Files.readAllBytes(corruptPath);
        bytes[bytes.length - 1] ^= 0x55;
        Files.write(corruptPath, bytes);
        assertThrows(DatabaseFatalException.class,
                () -> FileOnlineAlterChangeLog.open(corruptPath, 64 * 1024, 4096));
    }

    /** 两遍reconciliation按candidate游标分批读取；state/watermark不占limit，rewind后顺序完全相同。 */
    @Test
    void streamsOnlyCandidatesInBoundedRewindableBatches() {
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                path(), header(), 64 * 1024, 4096)) {
            startCapture(log);
            for (int value = 1; value <= 5; value++) {
                long sequence = log.appendCandidate(
                        TransactionId.of(100 + value), new byte[]{(byte) value});
                if (value == 3) {
                    log.forceThrough(sequence, Duration.ofSeconds(2));
                }
            }

            List<Integer> firstPass = readCandidateValues(log, 2);
            List<Integer> secondPass = readCandidateValues(log, 2);
            assertEquals(List.of(1, 2, 3, 4, 5), firstPass);
            assertEquals(firstPass, secondPass);
        }
    }

    /** Shadow READY 必须持久保存不早于capture baseline的final ReadView generation并可在重开后解释。 */
    @Test
    void persistsShadowReadyReadViewGeneration() {
        Path path = directory.resolve("online-alter-82.log");
        OnlineAlterLogHeader shadowHeader = new OnlineAlterLogHeader(
                OnlineDdlCaptureId.of(82), 22, 33, 34,
                7, 8, 5, 1025, 9,
                "shadow-manifest".getBytes(StandardCharsets.UTF_8));
        try (FileOnlineAlterChangeLog log = FileOnlineAlterChangeLog.create(
                path, shadowHeader, 64 * 1024, 4096)) {
            startCapture(log);
            log.appendState(OnlineAlterLogRecordType.SEALED, new byte[0]);
            byte[] generation = ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.BIG_ENDIAN).putLong(11).array();
            long ready = log.appendState(
                    OnlineAlterLogRecordType.READY_TO_PUBLISH, generation);
            log.forceThrough(ready, Duration.ofSeconds(2));
        }

        try (FileOnlineAlterChangeLog reopened = FileOnlineAlterChangeLog.open(
                path, 64 * 1024, 4096)) {
            byte[] payload = reopened.readAll().stream()
                    .filter(record -> record.type()
                            == OnlineAlterLogRecordType.READY_TO_PUBLISH)
                    .findFirst().orElseThrow().payload();
            assertEquals(11L, ByteBuffer.wrap(payload)
                    .order(ByteOrder.BIG_ENDIAN).getLong());
        }
    }

    private static List<Integer> readCandidateValues(
            FileOnlineAlterChangeLog log, int batchSize) {
        List<Integer> values = new ArrayList<>();
        long cursor = 0L;
        while (true) {
            var batch = log.readCandidatesAfter(cursor, batchSize);
            if (batch.isEmpty()) {
                return List.copyOf(values);
            }
            batch.forEach(record -> values.add(Byte.toUnsignedInt(record.payload()[0])));
            cursor = batch.getLast().sequence();
        }
    }

    private Path path() {
        return directory.resolve("online-alter-81.log");
    }

    private static void startCapture(FileOnlineAlterChangeLog log) {
        log.appendState(OnlineAlterLogRecordType.GENERATION_STARTED, new byte[0]);
        log.appendState(OnlineAlterLogRecordType.CAPTURING, new byte[0]);
    }

    private static OnlineAlterLogHeader header() {
        return new OnlineAlterLogHeader(OnlineDdlCaptureId.of(81), 22,
                33, 34, 7, 7, 4, 0, 0,
                "general-manifest".getBytes(StandardCharsets.UTF_8));
    }
}
