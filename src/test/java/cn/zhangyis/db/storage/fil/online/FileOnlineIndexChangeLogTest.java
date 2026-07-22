package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineChangeLogSnapshot;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FileChannel row log 的 owner、force、reopen、尾截断与容量终态测试。 */
class FileOnlineIndexChangeLogTest {

    @TempDir
    Path directory;

    /** 正Duration超出long纳秒范围时文件锁等待应饱和，不能在force前抛裸算术溢出。 */
    @Test
    void shouldAcceptPositiveForceTimeoutLargerThanLongNanos() {
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path(), header(), 64 * 1024, 4096)) {
            startCapture(log);
            long sequence = log.appendCandidate(TransactionId.of(70), new byte[]{1});

            log.forceThrough(sequence, Duration.ofSeconds(Long.MAX_VALUE));

            assertTrue(log.highestForcedSequence() >= sequence);
        }
    }

    /** candidate force 后重开必须恢复 header、记录、高水位和完整 payload。 */
    @Test
    void shouldPersistAndReopenForcedCandidate() {
        Path path = path();
        long sequence;
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            startCapture(log);
            sequence = log.appendCandidate(TransactionId.of(71),
                    "entry".getBytes(StandardCharsets.UTF_8));
            log.forceThrough(sequence, Duration.ofSeconds(2));
            assertTrue(log.highestForcedSequence() >= sequence);
        }

        try (FileOnlineIndexChangeLog reopened = FileOnlineIndexChangeLog.open(
                path, 64 * 1024, 4096)) {
            assertEquals(header().buildId(), reopened.header().buildId());
            assertTrue(reopened.highestForcedSequence() >= sequence);
            assertEquals(OnlineIndexLogRecordType.CANDIDATE,
                    reopened.readAll().stream()
                            .filter(record -> record.sequence() == sequence)
                            .findFirst().orElseThrow().type());
        }
    }

    /** 只有最后一个未完成 frame 可以在 reopen 时截断，之前已force记录必须保留。 */
    @Test
    void shouldTruncateIncompleteTailOnly() throws Exception {
        Path path = path();
        long stableSize;
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            startCapture(log);
            long sequence = log.appendCandidate(TransactionId.of(72), new byte[]{1, 2, 3});
            log.forceThrough(sequence, Duration.ofSeconds(2));
            stableSize = Files.size(path);
        }
        Files.write(path, new byte[]{0, 0, 0, 80, 1, 2, 3},
                java.nio.file.StandardOpenOption.APPEND);

        try (FileOnlineIndexChangeLog reopened = FileOnlineIndexChangeLog.open(
                path, 64 * 1024, 4096)) {
            assertEquals(stableSize, Files.size(path));
            assertFalse(reopened.readAll().isEmpty());
        }
    }

    /** candidate 不能消费 abort reserve；溢出必须在同一调用内持久化 ABORT_REQUIRED 并让 DML 继续。 */
    @Test
    void shouldReserveCapacityForDurableAbort() {
        Path path = path();
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 8192, 4096)) {
            startCapture(log);
            byte[] large = new byte[5000];

            long sequence = log.appendCandidate(TransactionId.of(73), large);

            assertEquals(0, sequence);
            assertTrue(log.abortRequired());
            assertTrue(log.highestForcedSequence() > 0);
        }
    }

    /** terminal reserve 还要容纳最后一次 force watermark；过小配置必须在创建文件前拒绝。 */
    @Test
    void shouldRejectReserveThatCannotHoldForceAndAbortFrames() {
        assertThrows(DatabaseValidationException.class, () -> FileOnlineIndexChangeLog.create(
                path(), header(), 8192, 64));
    }

    /** 完整 frame 的 CRC 损坏不是可截断尾部，恢复必须 fail-closed。 */
    @Test
    void shouldRejectCorruptedCompleteFrame() throws Exception {
        Path path = path();
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            startCapture(log);
            log.appendCandidate(TransactionId.of(74), "entry".getBytes(StandardCharsets.UTF_8));
        }
        byte[] bytes = Files.readAllBytes(path);
        bytes[bytes.length - 1] ^= 0x7f;
        Files.write(path, bytes);

        assertThrows(DatabaseFatalException.class,
                () -> FileOnlineIndexChangeLog.open(path, 64 * 1024, 4096));
    }

    /** CRC 正确也不能绕过 frame 状态机；未建立 generation/capture 的 candidate 必须 fail-closed。 */
    @Test
    void shouldRejectCandidateOutsideCapturingInterval() throws Exception {
        Path path = path();
        try (FileOnlineIndexChangeLog ignored = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            // 只持久 header，模拟非本实现写入的格式有效但状态非法 frame。
        }
        byte[] invalid = new OnlineIndexRowLogCodec().encodeRecord(
                new OnlineIndexLogRecord(OnlineIndexLogRecordType.CANDIDATE,
                        1, 1, 75, new byte[]{1}));
        Files.write(path, invalid, java.nio.file.StandardOpenOption.APPEND);

        assertThrows(DatabaseFatalException.class,
                () -> FileOnlineIndexChangeLog.open(path, 64 * 1024, 4096));
    }

    /** 多个提交者并发 force 各自高水位时，所有 follower 都只能在覆盖目标后返回。 */
    @Test
    void shouldCoverConcurrentForceFollowers() throws Exception {
        Path path = path();
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096);
             ExecutorService executor = Executors.newFixedThreadPool(4)) {
            startCapture(log);
            List<Long> sequences = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                sequences.add(log.appendCandidate(TransactionId.of(80 + i), new byte[]{(byte) i}));
            }
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (long sequence : sequences) {
                futures.add(executor.submit(() -> {
                    start.await();
                    log.forceThrough(sequence, Duration.ofSeconds(2));
                    assertTrue(log.highestForcedSequence() >= sequence);
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }

    /** 高频只读快照必须在同一文件锁内复制全部标量，不能通过多 getter 组合出倒退的 size/sequence/count。 */
    @Test
    void shouldPublishMonotonicSingleLockSnapshotsDuringAppend() throws Exception {
        Path path = path();
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            startCapture(log);
            Future<?> writer = executor.submit(() -> {
                for (int index = 0; index < 100; index++) {
                    log.appendCandidate(TransactionId.of(1_000 + index), new byte[]{(byte) index});
                }
            });
            long candidates = 0;
            long size = 0;
            long appended = 0;
            while (!writer.isDone()) {
                OnlineChangeLogSnapshot snapshot = log.snapshot();
                assertTrue(snapshot.candidateCount() >= candidates);
                assertTrue(snapshot.sizeBytes() >= size);
                assertTrue(snapshot.highestAppendedSequence() >= appended);
                assertTrue(snapshot.highestForcedSequence()
                        <= snapshot.highestAppendedSequence());
                candidates = snapshot.candidateCount();
                size = snapshot.sizeBytes();
                appended = snapshot.highestAppendedSequence();
            }
            writer.get();

            OnlineChangeLogSnapshot completed = log.snapshot();
            assertEquals(100, completed.candidateCount());
            assertEquals(Files.size(path), completed.sizeBytes());
            assertEquals(1, completed.generation());
            assertTrue(completed.capturing());
            assertFalse(completed.closed());
        }
    }

    /** crash reset 只保留 immutable manifest，并使用新 generation 从空序列重扫。 */
    @Test
    void shouldResetToManifestWithNewGeneration() {
        Path path = path();
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            startCapture(log);
            log.appendCandidate(TransactionId.of(90), new byte[]{1});

            log.resetToManifest(Duration.ofSeconds(2));
            startCapture(log);
            long sequence = log.appendState(OnlineIndexLogRecordType.SEALED, new byte[0]);

            assertEquals(3, sequence);
            assertEquals(2, log.readAll().getFirst().generation());
        }
    }

    /** RECONCILED 是发布不可回退点；即使调用方误走 abort API，也不能在其后追加相反裁决。 */
    @Test
    void shouldRejectAbortAfterReconciliation() {
        Path path = path();
        try (FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                path, header(), 64 * 1024, 4096)) {
            startCapture(log);
            log.appendState(OnlineIndexLogRecordType.SEALED, new byte[0]);
            long reconciled = log.appendState(OnlineIndexLogRecordType.RECONCILED, new byte[0]);
            log.forceThrough(reconciled, Duration.ofSeconds(2));

            assertThrows(DatabaseFatalException.class, () -> log.markAbortRequired(
                    cn.zhangyis.db.storage.api.ddl.online.OnlineDdlAbortReason.VALIDATION_FAILED,
                    Duration.ofSeconds(2)));
            assertFalse(log.abortRequired());
        }
    }

    private Path path() {
        return directory.resolve("online-index-11.log");
    }

    /** 建立生产 target 发布前要求的 generation/capturing 状态。 */
    private static void startCapture(FileOnlineIndexChangeLog log) {
        log.appendState(OnlineIndexLogRecordType.GENERATION_STARTED, new byte[0]);
        log.appendState(OnlineIndexLogRecordType.CAPTURING, new byte[0]);
    }

    private static OnlineIndexLogHeader header() {
        return new OnlineIndexLogHeader(OnlineIndexBuildId.of(11), 22, 33,
                44, 45, 7, "manifest".getBytes(StandardCharsets.UTF_8));
    }
}
