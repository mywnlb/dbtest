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
