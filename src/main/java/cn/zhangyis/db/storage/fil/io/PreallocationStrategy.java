package cn.zhangyis.db.storage.fil.io;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 平台相关稀疏文件/磁盘空间预留的 package-private 策略端口。
 *
 * <p>{@link PreallocatingDataFileGateway} 在完成页范围和乘法溢出校验后传入字节区间，并且无论策略
 * 是否真正预留空间，随后仍由 {@link ZeroFillDataFileGateway} 写零建立确定页内容。因此策略不能把
 * preallocate 成功解释为页面初始化完成，也不负责发布文件大小。</p>
 *
 * <p>实现只允许操作给定 channel 的指定范围，不得重新解析表空间、回调 Buffer Pool/registry/redo。
 * 当前生产没有 native 实现，只有 no-op 与测试替身；该端口尚不改变默认零填充路径。</p>
 */
interface PreallocationStrategy {

    /**
     * 尝试为指定字节范围预留底层存储空间。
     *
     * <p>允许实现为 no-op。成功不保证范围内字节值，也不执行 force；失败必须保留底层原因，
     * 使 gateway 停止后续可见大小发布。</p>
     *
     * @param channel 已打开、由 DataFileHandle 持锁保护的可写 data-file channel
     * @param offsetBytes 已校验的非负起始字节偏移
     * @param lengthBytes 已校验的非负范围长度；零表示空范围
     * @param path channel 对应的非空诊断路径
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 平台预留调用失败时抛出并保留原因
     */
    void preallocate(FileChannel channel, long offsetBytes, long lengthBytes, Path path);
}
