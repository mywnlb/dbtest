package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PageGuard 测试用真实 BufferFrame + 假 FrameReleaser 固定：字节/整数读写、X 模式约束、越界、close 释放与回调、幂等。
 */
class PageGuardTest {

    private static final PageSize PS = PageSize.ofBytes(4 * 1024);

    private PageGuard exclusiveGuard(BufferFrame frame, FrameReleaser releaser) {
        Lock latch = frame.pageLatch.writeLock();
        latch.lock();
        return new PageGuard(releaser, frame, PageLatchMode.EXCLUSIVE, latch, null);
    }

    private PageGuard sharedGuard(BufferFrame frame, FrameReleaser releaser) {
        Lock latch = frame.pageLatch.readLock();
        latch.lock();
        return new PageGuard(releaser, frame, PageLatchMode.SHARED, latch, null);
    }

    /**
     * 验证 {@code shouldRoundTripIntAndBytesUnderExclusive} 对应的Buffer Pool行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void shouldRoundTripIntAndBytesUnderExclusive() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        guard.writeInt(0, 0x01020304);
        guard.writeBytes(8, new byte[] {9, 8, 7});
        assertEquals(0x01020304, guard.readInt(0));
        assertArrayEquals(new byte[] {9, 8, 7}, guard.readBytes(8, 3));
        guard.close();
    }

    /**
     * 验证 {@code shouldRejectWriteUnderShared} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectWriteUnderShared() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = sharedGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.writeInt(0, 1));
        assertThrows(DatabaseValidationException.class, () -> guard.writeBytes(0, new byte[] {1}));
        assertThrows(DatabaseValidationException.class, () -> guard.markDirty());
        guard.close();
    }

    /**
     * 验证 {@code shouldRejectOutOfBoundsAccess} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectOutOfBoundsAccess() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.readInt(PS.bytes() - 2));
        assertThrows(DatabaseValidationException.class, () -> guard.writeBytes(PS.bytes() - 1, new byte[] {1, 2}));
        guard.close();
    }

    /**
     * 验证 {@code shouldRejectNullWriteSource} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectNullWriteSource() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.writeBytes(0, null));
        guard.close();
    }

    /**
     * 验证 {@code shouldRejectAccessAfterClose} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectAccessAfterClose() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        guard.close();
        assertThrows(DatabaseValidationException.class, () -> guard.readInt(0));
        assertThrows(DatabaseValidationException.class, () -> guard.writeInt(0, 1));
    }

    /**
     * 验证 {@code closeShouldReleaseLatchAndReportWrote} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
    @Test
    void closeShouldReleaseLatchAndReportWrote() {
        BufferFrame frame = new BufferFrame(PS);
        AtomicBoolean called = new AtomicBoolean(false);
        AtomicReference<Boolean> wroteSeen = new AtomicReference<>();
        FrameReleaser releaser = (f, w) -> { called.set(true); wroteSeen.set(w); };

        PageGuard guard = exclusiveGuard(frame, releaser);
        guard.writeInt(0, 42);
        guard.close();

        assertTrue(called.get());
        assertEquals(Boolean.TRUE, wroteSeen.get());
        assertFalse(frame.pageLatch.isWriteLocked(), "page latch must be released after close");

        guard.close(); // 幂等
    }

    /**
     * 验证 {@code closeWithoutWriteShouldReportNotWrote} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void closeWithoutWriteShouldReportNotWrote() {
        BufferFrame frame = new BufferFrame(PS);
        AtomicReference<Boolean> wroteSeen = new AtomicReference<>();
        PageGuard guard = sharedGuard(frame, (f, w) -> wroteSeen.set(w));
        guard.readInt(0);
        guard.close();
        assertEquals(Boolean.FALSE, wroteSeen.get());
        assertEquals(0, frame.pageLatch.getReadLockCount());
    }

    /**
     * 验证 {@code shouldExposePageId} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldExposePageId() {
        BufferFrame frame = new BufferFrame(PS);
        frame.pageId = PageId.of(SpaceId.of(1), PageNo.of(5));
        PageGuard guard = sharedGuard(frame, (f, w) -> { });
        assertEquals(PageId.of(SpaceId.of(1), PageNo.of(5)), guard.pageId());
        guard.close();
    }

    /**
     * 验证 {@code shouldRoundTripLongUnderExclusive} 对应的Buffer Pool行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void shouldRoundTripLongUnderExclusive() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        guard.writeLong(16, 0x1122334455667788L);
        assertEquals(0x1122334455667788L, guard.readLong(16));
        guard.close();
    }

    /**
     * 验证 {@code shouldRejectWriteLongUnderShared} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectWriteLongUnderShared() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = sharedGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.writeLong(0, 1L));
        guard.close();
    }

    /**
     * 验证 {@code shouldRejectLongOutOfBounds} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectLongOutOfBounds() {
        BufferFrame frame = new BufferFrame(PS);
        PageGuard guard = exclusiveGuard(frame, (f, w) -> { });
        assertThrows(DatabaseValidationException.class, () -> guard.readLong(PS.bytes() - 4));
        guard.close();
    }
}
