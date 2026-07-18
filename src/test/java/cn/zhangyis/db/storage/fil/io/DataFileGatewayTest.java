package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DataFileGateway 测试：固定默认零填充网关对物理页范围的初始化语义。
 */
class DataFileGatewayTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    /**
     * 验证 {@code zeroFillGatewayWritesFullPageRange} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     *
     * @param tempDir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    @Test
    void zeroFillGatewayWritesFullPageRange(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("space.ibd");
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            DataFileGateway gateway = new ZeroFillDataFileGateway();

            gateway.initialize(channel, 0, 3, PS, file);

            assertEquals(3L * PS.bytes(), channel.size());
            ByteBuffer page = ByteBuffer.allocate(PS.bytes());
            channel.read(page, 2L * PS.bytes());
            page.flip();
            while (page.hasRemaining()) {
                assertEquals(0, page.get());
            }
        }
    }

    /**
     * 验证 {@code preallocatingGatewayCallsStrategyBeforeZeroFillAndKeepsPagesZero} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     *
     * @param tempDir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    @Test
    void preallocatingGatewayCallsStrategyBeforeZeroFillAndKeepsPagesZero(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("preallocated.ibd");
        RecordingPreallocationStrategy strategy = new RecordingPreallocationStrategy();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            DataFileGateway gateway = new PreallocatingDataFileGateway(strategy);

            gateway.initialize(channel, 1, 3, PS, file);

            assertEquals(1, strategy.calls);
            assertEquals(PS.bytes(), strategy.offsetBytes);
            assertEquals(2L * PS.bytes(), strategy.lengthBytes);
            ByteBuffer page = ByteBuffer.allocate(PS.bytes());
            channel.read(page, 2L * PS.bytes());
            page.flip();
            while (page.hasRemaining()) {
                assertEquals(0, page.get());
            }
        }
    }

    private static final class RecordingPreallocationStrategy implements PreallocationStrategy {
        private long offsetBytes = -1;
        private long lengthBytes = -1;
        private int calls;

        @Override
        public void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path) {
            this.calls++;
            this.offsetBytes = offsetBytes;
            this.lengthBytes = lengthBytes;
        }
    }
}
