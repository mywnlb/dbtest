package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DataFileHandle gateway 接线测试：文件创建、扩展和 ensureCapacity 必须通过 gateway 初始化范围，
 * 且 gateway 失败时不能发布新的 currentSizeInPages。
 */
class DataFileHandleGatewayTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    /**
     * 验证 {@code createDelegatesInitializationToGateway} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     *
     * @param tempDir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     */
    @Test
    void createDelegatesInitializationToGateway(@TempDir Path tempDir) {
        RecordingGateway gateway = new RecordingGateway();

        try (DataFileHandle handle = DataFileHandle.create(SPACE, tempDir.resolve("space.ibd"),
                PS, PageNo.of(4), gateway)) {
            assertEquals(1, gateway.calls);
            assertEquals(0, gateway.from);
            assertEquals(4, gateway.to);
            assertEquals(4, handle.currentSizeInPages());
        }
    }

    /**
     * 验证 {@code autoExtendDoesNotPublishSizeWhenGatewayFails} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @param tempDir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     */
    @Test
    void autoExtendDoesNotPublishSizeWhenGatewayFails(@TempDir Path tempDir) {
        DataFileGateway failing = new DataFileGateway() {
            @Override
            public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                   PageSize pageSize, Path path) {
                new ZeroFillDataFileGateway().initialize(channel, fromPageInclusive, toPageExclusive,
                        pageSize, path);
            }

            @Override
            public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                        PageSize pageSize, Path path) {
                throw new DataFilePhysicalException("injected gateway failure");
            }
        };

        try (DataFileHandle handle = DataFileHandle.create(SPACE, tempDir.resolve("space.ibd"),
                PS, PageNo.of(2), failing)) {
            assertThrows(DataFilePhysicalException.class,
                    () -> handle.autoExtend((current, pageSize) -> 1));
            assertEquals(2, handle.currentSizeInPages());
        }
    }

    /**
     * 验证 {@code ensureCapacityUsesGatewayOnlyWhenGrowing} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     *
     * @param tempDir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     */
    @Test
    void ensureCapacityUsesGatewayOnlyWhenGrowing(@TempDir Path tempDir) {
        RecordingGateway gateway = new RecordingGateway();

        try (DataFileHandle handle = DataFileHandle.create(SPACE, tempDir.resolve("space.ibd"),
                PS, PageNo.of(2), gateway)) {
            gateway.calls = 0;

            handle.ensureCapacity(PageNo.of(5));

            assertEquals(1, gateway.calls);
            assertEquals(2, gateway.from);
            assertEquals(5, gateway.to);
            handle.ensureCapacity(PageNo.of(5));
            handle.ensureCapacity(PageNo.of(3));
            assertEquals(1, gateway.calls);
        }
    }

    private static final class RecordingGateway implements DataFileGateway {
        private long from = -1;
        private long to = -1;
        private int calls;

        @Override
        public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                               PageSize pageSize, Path path) {
            calls++;
            from = fromPageInclusive;
            to = toPageExclusive;
            new ZeroFillDataFileGateway().initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
        }

        @Override
        public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                    PageSize pageSize, Path path) {
            initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
        }
    }
}
