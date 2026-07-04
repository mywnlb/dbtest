package cn.zhangyis.db.storage.flush.cleaner;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.FlushResultStatus;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PageCleaner supervisor 切片测试：先固定成功 cycle 的 metrics，再逐步扩展失败重启语义。
 */
class PageCleanerSupervisorTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));
    private static final PageId REDO_ONLY_PAGE = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    @Test
    void supervisorReportsMetricsAfterSuccessfulCycle() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
            RedoLogManager redo = RedoLogManager.durable(repo);
            Lsn dirtyLsn = appendDirtyThenGrowRedo(redo);
            writeDirty(pool, PAGE, dirtyLsn);
            FlushService service = service(pool, store, redo);
            PageCleanerWorkerFactory factory = () -> new PageCleanerWorker(service, 4, Duration.ofMillis(5));

            try (PageCleanerSupervisor supervisor = new PageCleanerSupervisor(
                    factory, 1, Duration.ofMillis(1), Duration.ofMillis(5))) {
                supervisor.start();
                supervisor.requestFlush(10);

                assertTrue(supervisor.awaitIdle(Duration.ofSeconds(2)));
                PageCleanerMetricsSnapshot snapshot = supervisor.metricsSnapshot();
                assertEquals(PageCleanerState.IDLE, snapshot.state());
                assertEquals(1, snapshot.successfulCycles());
                assertEquals(0, snapshot.failedCycles());
                assertEquals(0, snapshot.restartCount());
                assertTrue(snapshot.lastCyclePresent());
                assertEquals("", snapshot.lastErrorMessage());
                assertTrue(supervisor.lastCycle().orElseThrow().results().stream()
                        .anyMatch(result -> result.status() == FlushResultStatus.CLEAN));
            }
        }
    }

    @Test
    void supervisorRestartsFailedWorkerWithinLimit() throws Exception {
        ScriptedWorker failed = ScriptedWorker.failsOnRequest("first failure");
        ScriptedWorker recovered = ScriptedWorker.succeedsOnRequest();
        AtomicInteger created = new AtomicInteger();
        PageCleanerWorkerFactory factory = () -> created.incrementAndGet() == 1 ? failed : recovered;

        try (PageCleanerSupervisor supervisor = new PageCleanerSupervisor(
                factory, 1, Duration.ofMillis(1), Duration.ofMillis(5))) {
            supervisor.start();
            supervisor.requestFlush(1);

            assertTrue(recovered.started.await(1, TimeUnit.SECONDS), "supervisor did not restart failed worker");
            supervisor.requestFlush(1);
            assertTrue(supervisor.awaitIdle(Duration.ofSeconds(1)));
            PageCleanerMetricsSnapshot snapshot = supervisor.metricsSnapshot();
            assertEquals(1, snapshot.restartCount());
            assertEquals(1, snapshot.failedCycles());
            assertEquals(1, snapshot.successfulCycles());
            assertEquals("first failure", snapshot.lastErrorMessage());
            assertEquals(PageCleanerState.IDLE, snapshot.state());
        }
    }

    @Test
    void supervisorMetricsRememberHistoricalLastCycleAfterRestart() throws Exception {
        ScriptedWorker first = ScriptedWorker.succeedsOnRequest();
        ScriptedWorker replacement = ScriptedWorker.succeedsOnRequest();
        AtomicInteger created = new AtomicInteger();
        PageCleanerWorkerFactory factory = () -> created.incrementAndGet() == 1 ? first : replacement;

        try (PageCleanerSupervisor supervisor = new PageCleanerSupervisor(
                factory, 1, Duration.ofMillis(1), Duration.ofMillis(5))) {
            supervisor.start();
            supervisor.requestFlush(1);
            assertTrue(supervisor.awaitIdle(Duration.ofSeconds(1)));
            assertTrue(supervisor.metricsSnapshot().lastCyclePresent());

            first.failNextRequest("restart after success");
            supervisor.requestFlush(1);
            assertTrue(replacement.started.await(1, TimeUnit.SECONDS), "supervisor did not install replacement");

            PageCleanerMetricsSnapshot snapshot = supervisor.metricsSnapshot();
            assertEquals(1, snapshot.successfulCycles());
            assertEquals(1, snapshot.failedCycles());
            assertEquals(1, snapshot.restartCount());
            assertTrue(snapshot.lastCyclePresent(),
                    "lastCyclePresent 表达当前或历史 worker 已有成功 cycle，不能被新 worker 空快照清掉");
        }
    }

    @Test
    void supervisorStopsAfterRestartLimitExceeded() throws Exception {
        ScriptedWorker first = ScriptedWorker.failsOnRequest("failure-1");
        ScriptedWorker second = ScriptedWorker.failsOnRequest("failure-2");
        AtomicInteger created = new AtomicInteger();
        PageCleanerWorkerFactory factory = () -> created.incrementAndGet() == 1 ? first : second;

        try (PageCleanerSupervisor supervisor = new PageCleanerSupervisor(
                factory, 1, Duration.ofMillis(1), Duration.ofMillis(5))) {
            supervisor.start();
            supervisor.requestFlush(1);
            assertTrue(second.started.await(1, TimeUnit.SECONDS), "supervisor did not restart first failed worker");
            supervisor.requestFlush(1);

            assertTrue(supervisor.awaitState(PageCleanerState.FAILED, Duration.ofSeconds(1)));
            PageCleanerMetricsSnapshot snapshot = supervisor.metricsSnapshot();
            assertEquals(PageCleanerState.FAILED, snapshot.state());
            assertEquals(1, snapshot.restartCount());
            assertEquals(2, snapshot.failedCycles());
            assertEquals("failure-2", snapshot.lastErrorMessage());
            assertThrows(PageCleanerStoppedException.class, () -> supervisor.requestFlush(1));
        }
    }

    private FlushService service(BufferPool pool, PageStore store, RedoLogManager redo) {
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        CheckpointCoordinator checkpoint = new CheckpointCoordinator(pool, redo);
        return new FlushService(pool, coordinator, checkpoint, redo, RedoCapacityPolicy.fixed(100),
                AdaptiveFlushPolicy.fixed(1, 8));
    }

    private Lsn appendDirtyThenGrowRedo(RedoLogManager redo) {
        Lsn dirty = appendOneRedo(redo, PAGE);
        for (int i = 0; i < 8; i++) {
            appendOneRedo(redo, REDO_ONLY_PAGE);
        }
        redo.flush();
        return dirty;
    }

    private static Lsn appendOneRedo(RedoLogManager redo, PageId pageId) {
        Lsn lsn = redo.append(List.of(new PageBytesRecord(pageId, 256, new byte[]{1, 2, 3}))).end();
        redo.flush();
        return lsn;
    }

    private static void writeDirty(BufferPool pool, PageId pageId, Lsn lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
            guard.writeBytes(256, new byte[]{1, 2, 3});
        }
    }

    /** 可脚本化 worker，专用于 supervisor 重启测试；避免构造真实 IO 失败路径污染测试意图。 */
    private static final class ScriptedWorker implements PageCleanerWorkerHandle {
        private final boolean failOnRequest;
        private final String failureMessage;
        private final CountDownLatch started = new CountDownLatch(1);
        private PageCleanerState state = PageCleanerState.NEW;
        private long completedCycles;
        private boolean failNextRequest;
        private String nextFailureMessage = "";

        private ScriptedWorker(boolean failOnRequest, String failureMessage) {
            this.failOnRequest = failOnRequest;
            this.failureMessage = failureMessage;
        }

        private static ScriptedWorker failsOnRequest(String message) {
            return new ScriptedWorker(true, message);
        }

        private static ScriptedWorker succeedsOnRequest() {
            return new ScriptedWorker(false, "");
        }

        private void failNextRequest(String message) {
            failNextRequest = true;
            nextFailureMessage = message;
        }

        @Override
        public void start() {
            state = PageCleanerState.IDLE;
            started.countDown();
        }

        @Override
        public void requestFlush(int maxPages) {
            if (failOnRequest || failNextRequest) {
                state = PageCleanerState.FAILED;
                failNextRequest = false;
                return;
            }
            completedCycles++;
            state = PageCleanerState.IDLE;
        }

        @Override
        public boolean awaitIdle(Duration timeout) {
            return state == PageCleanerState.IDLE || state == PageCleanerState.FAILED || state == PageCleanerState.STOPPED;
        }

        @Override
        public boolean stop(Duration timeout) {
            state = PageCleanerState.STOPPED;
            return true;
        }

        @Override
        public PageCleanerState state() {
            return state;
        }

        @Override
        public Optional<cn.zhangyis.db.storage.flush.FlushCycleResult> lastCycle() {
            return Optional.empty();
        }

        @Override
        public PageCleanerWorkerSnapshot snapshot() {
            String message = nextFailureMessage.isBlank() ? failureMessage : nextFailureMessage;
            return new PageCleanerWorkerSnapshot(state, false, 0, completedCycles,
                    completedCycles > 0, state == PageCleanerState.FAILED ? message : "");
        }

        @Override
        public void close() {
            stop(Duration.ZERO);
        }
    }
}
