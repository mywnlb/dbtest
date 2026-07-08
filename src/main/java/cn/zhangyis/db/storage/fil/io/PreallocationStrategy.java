package cn.zhangyis.db.storage.fil.io;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 平台预分配策略。实现只允许操作传入的 FileChannel 范围，不得回调数据库上层模块；
 * 不支持 native 预分配的平台应选择 no-op 或在策略内部降级，而不是改变 DataFileGateway 的零填充保证。
 */
interface PreallocationStrategy {

    /**
     * 尝试预分配指定字节范围。该方法不保证文件内容；调用方仍必须零填充新页范围。
     *
     * @param channel 已打开的 data file channel。
     * @param offsetBytes 起始字节偏移。
     * @param lengthBytes 长度字节数。
     * @param path 文件路径，仅用于诊断。
     */
    void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path);
}
