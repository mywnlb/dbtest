package cn.zhangyis.db.storage.fil.io;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 不执行 native 空间预留的跨平台策略。
 *
 * <p>所有参数均由 {@link PreallocatingDataFileGateway} 在调用前校验；本实现不读取 channel、
 * 不改变文件长度，也不把零长度区间视为错误。随后执行的 {@link ZeroFillDataFileGateway}
 * 仍负责真正建立文件范围和确定的零内容。</p>
 */
final class NoOpPreallocationStrategy implements PreallocationStrategy {

    /**
     * 接受预留请求但不产生物理副作用。
     *
     * @param channel 已打开 channel；本实现不访问
     * @param offsetBytes 非负范围起点；本实现不访问
     * @param lengthBytes 非负范围长度；本实现不访问
     * @param path 诊断路径；本实现不访问
     */
    @Override
    public void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path) {
        // 有意 no-op；adapter 随后的零填充才负责文件长度与内容。
    }
}
