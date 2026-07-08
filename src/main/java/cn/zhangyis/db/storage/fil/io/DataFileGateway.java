package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * data-file 物理范围初始化网关。调用方已经持有 {@link DataFileHandle} 的生命周期/文件大小锁；
 * 本接口只处理 FileChannel 上的 page range 初始化或预分配，不回调 Buffer Pool、registry、redo 或 flush。
 */
interface DataFileGateway {

    /**
     * 初始化新建文件的页范围。范围按页号左闭右开表达，必须在句柄发布前完成，确保新文件所有可见页可读为确定内容。
     *
     * @param channel 已打开的 data file channel。
     * @param fromPageInclusive 起始页号（包含）。
     * @param toPageExclusive 结束页号（不包含）。
     * @param pageSize 实例页大小。
     * @param path 文件路径，仅用于诊断。
     */
    void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path);

    /**
     * 确保已存在文件的页范围已物理分配并初始化。调用成功返回后，DataFileHandle 才允许发布新的逻辑页数。
     *
     * @param channel 已打开的 data file channel。
     * @param fromPageInclusive 起始页号（包含）。
     * @param toPageExclusive 结束页号（不包含）。
     * @param pageSize 实例页大小。
     * @param path 文件路径，仅用于诊断。
     */
    void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                         PageSize pageSize, Path path);
}
