package cn.zhangyis.db.storage.fil.io;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 默认预分配策略：不调用任何平台 native API。它保留 adapter seam，但实际内容确定性仍由
 * {@link ZeroFillDataFileGateway} 完成。
 */
final class NoOpPreallocationStrategy implements PreallocationStrategy {

    @Override
    public void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path) {
        // 默认跨平台实现不做 native 预分配；DataFileGateway 仍会零填充保证新页内容确定。
    }
}
