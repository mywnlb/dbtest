package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteMode;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteStrategy;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 淘汰刷盘适配器测试：把 {@link FlushCoordinator#singlePageFlush} 的结果映射成淘汰端口契约——
 * CLEAN→true 并落盘、redo 未 durable→false 且不写盘、FAILED→抛出携带的根因（不被吞）。
 */
class CoordinatedDirtyVictimFlusherTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    /**
     * 验证 {@code returnsTrueAndWritesPageWhenRedoDurable} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void returnsTrueAndWritesPageWhenRedoDurable() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            long endLsn = appendRedoCoveringPage(redo);
            markDirty(pool, endLsn, 0xCB);
            redo.flush();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofMillis(50));
            CoordinatedDirtyVictimFlusher flusher = new CoordinatedDirtyVictimFlusher(coordinator);

            assertTrue(flusher.flushVictim(PAGE));

            byte[] disk = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(disk));
            assertEquals((byte) 0xCB, disk[200]);
        }
    }

    /**
     * 验证 {@code returnsFalseWhenRedoNotDurable} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void returnsFalseWhenRedoNotDurable() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            markDirty(pool, 100, 0xCB);
            RedoLogManager redo = new RedoLogManager();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofMillis(20));
            CoordinatedDirtyVictimFlusher flusher = new CoordinatedDirtyVictimFlusher(coordinator);

            assertFalse(flusher.flushVictim(PAGE));

            byte[] disk = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(disk));
            assertEquals(0, disk[200]);
        }
    }

    /**
     * 验证 {@code throwsCarriedCauseWhenFlushFails} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void throwsCarriedCauseWhenFlushFails() {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 4);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            long endLsn = appendRedoCoveringPage(redo);
            markDirty(pool, endLsn, 0xCB);
            redo.flush();
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new ThrowingDoublewriteStrategy(), Duration.ofMillis(50));
            CoordinatedDirtyVictimFlusher flusher = new CoordinatedDirtyVictimFlusher(coordinator);

            DatabaseRuntimeException ex = assertThrows(DatabaseRuntimeException.class,
                    () -> flusher.flushVictim(PAGE));
            assertTrue(ex.getMessage().contains("induced doublewrite"));
        }
    }

    private static void markDirty(BufferPool pool, long pageLsn, int marker) {
        try (PageGuard guard = pool.getPage(PAGE, PageLatchMode.EXCLUSIVE)) {
            guard.writeInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
            guard.writeInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
            guard.writeBytes(200, new byte[]{(byte) marker});
        }
    }

    private static long appendRedoCoveringPage(RedoLogManager redo) {
        PageBytesRecord record = new PageBytesRecord(PAGE, 200, new byte[]{(byte) 0xCB});
        return redo.append(List.of(record)).end().value();
    }

    /** 在 data file write 前抛领域异常，使 FlushCoordinator 产出 FAILED 结果，驱动适配器把根因向上抛。 */
    private static final class ThrowingDoublewriteStrategy implements DoublewriteStrategy {
        @Override
        public DoublewriteMode mode() {
            return DoublewriteMode.OFF;
        }

        @Override
        public void beforeDataFileWrite(FlushPageSnapshot snapshot) {
            throw new DatabaseRuntimeException("induced doublewrite failure");
        }
    }
}
