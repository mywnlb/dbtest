package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.DataFileCorruptedException;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;
import cn.zhangyis.db.storage.fil.exception.PageOutOfBoundsException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DataFileHandle 测试用真实临时文件固定物理读写/扩展/越界/关闭语义，不解析页内容。
 */
class DataFileHandleTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    /**
     * 验证 {@code shouldRoundTripPageBytes} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldRoundTripPageBytes() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(4))) {
            byte[] payload = pattern(PS.bytes(), (byte) 0xAB);
            handle.writePage(PageNo.of(2), ByteBuffer.wrap(payload));

            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            handle.readPage(PageNo.of(2), dst);
            assertArrayEquals(payload, dst.array());
        }
    }

    /**
     * 验证 {@code shouldRejectBufferWithWrongSize} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectBufferWithWrongSize() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(2))) {
            ByteBuffer tooSmall = ByteBuffer.allocate(PS.bytes() - 1);
            assertThrows(DatabaseValidationException.class, () -> handle.readPage(PageNo.of(0), tooSmall));
        }
    }

    /**
     * 验证 {@code shouldRejectOutOfBoundsAccess} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectOutOfBoundsAccess() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(2))) {
            ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
            assertThrows(PageOutOfBoundsException.class, () -> handle.readPage(PageNo.of(2), buf));
        }
    }

    /**
     * 验证 {@code shouldExtendZeroFilledAndMakeNewPagesReadable} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldExtendZeroFilledAndMakeNewPagesReadable() {
        Path file = dir.resolve("t.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(1))) {
            ByteBuffer before = ByteBuffer.allocate(PS.bytes());
            assertThrows(PageOutOfBoundsException.class, () -> handle.readPage(PageNo.of(1), before));

            long newSize = handle.autoExtend(new DefaultIbdAutoExtendPolicy());
            assertEquals(2, newSize);

            ByteBuffer after = ByteBuffer.allocate(PS.bytes());
            handle.readPage(PageNo.of(1), after);
            assertArrayEquals(new byte[PS.bytes()], after.array());
        }
    }

    /**
     * 验证 {@code shouldRejectNullAutoExtendPolicy} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectNullAutoExtendPolicy() {
        Path file = dir.resolve("nullpolicy.ibd");
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(1))) {
            assertThrows(DatabaseValidationException.class, () -> handle.autoExtend(null));
        }
    }

    /**
     * 验证 {@code shouldRejectCreateWhenFileExists} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    @Test
    void shouldRejectCreateWhenFileExists() throws IOException {
        Path file = dir.resolve("exists.ibd");
        Files.write(file, new byte[PS.bytes()]);
        assertThrows(DataFilePhysicalException.class, () -> DataFileHandle.create(SPACE, file, PS, PageNo.of(1)));
    }

    /**
     * 验证 {@code shouldRejectOpenWhenFileMissing} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectOpenWhenFileMissing() {
        Path file = dir.resolve("missing.ibd");
        assertThrows(DataFilePhysicalException.class, () -> DataFileHandle.open(SPACE, file, PS));
    }

    /**
     * 验证 {@code shouldRejectOpenWhenFileNotPageAligned} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    @Test
    void shouldRejectOpenWhenFileNotPageAligned() throws IOException {
        Path file = dir.resolve("misaligned.ibd");
        Files.write(file, new byte[PS.bytes() + 7]);
        assertThrows(DataFileCorruptedException.class, () -> DataFileHandle.open(SPACE, file, PS));
    }

    /**
     * 验证 {@code shouldDeriveSizeFromExistingFileLength} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     *
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    @Test
    void shouldDeriveSizeFromExistingFileLength() throws IOException {
        Path file = dir.resolve("sized.ibd");
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.allocate(PS.bytes() * 3));
        }
        try (DataFileHandle handle = DataFileHandle.open(SPACE, file, PS)) {
            assertEquals(3, handle.currentSizeInPages());
        }
    }

    /**
     * 验证 {@code shouldRejectIoAfterClose} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectIoAfterClose() {
        Path file = dir.resolve("closed.ibd");
        DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(2));
        handle.close();
        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
        assertThrows(TablespaceNotOpenException.class, () -> handle.readPage(PageNo.of(0), buf));
    }

    /**
     * 验证 {@code concurrentReadsDuringExtendShouldStayConsistent} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     *
     * @throws InterruptedException 等待被中断时抛出；调用方应恢复中断标志并终止当前资源获取流程
     */
    @Test
    void concurrentReadsDuringExtendShouldStayConsistent() throws InterruptedException {
        Path file = dir.resolve("concurrent.ibd");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        // 2000 次读 / 20 次扩展只是足够多次迭代以覆盖线程交叉的结构性探测，不是性能基准。
        try (DataFileHandle handle = DataFileHandle.create(SPACE, file, PS, PageNo.of(64))) {
            byte[] zeros = new byte[PS.bytes()];
            Thread reader = new Thread(() -> {
                try {
                    for (int i = 0; i < 2000; i++) {
                        // 读“当前已发布大小的最后一页”，真正行使 volatile 发布不变量：凡能被 size 快照覆盖的页，
                        // 其零填充必已完成；若发布早于零填充完成，这里会读到非零脏数据而断言失败。
                        long sz = handle.currentSizeInPages();
                        ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
                        handle.readPage(PageNo.of(sz - 1), buf);
                        assertArrayEquals(zeros, buf.array());
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            Thread extender = new Thread(() -> {
                try {
                    AutoExtendPolicy policy = new DefaultIbdAutoExtendPolicy();
                    long last = 64;
                    for (int i = 0; i < 20; i++) {
                        long s = handle.autoExtend(policy);
                        assertTrue(s >= last, "size must be monotonic");
                        last = s;
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            reader.start();
            extender.start();
            reader.join();
            extender.join();
        }
        assertNull(failure.get(), () -> "concurrent read/extend failed: " + failure.get());
    }

    private static byte[] pattern(int len, byte b) {
        byte[] a = new byte[len];
        java.util.Arrays.fill(a, b);
        return a;
    }
}
