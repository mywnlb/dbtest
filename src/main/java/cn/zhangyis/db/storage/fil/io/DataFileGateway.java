package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * data-file 页范围物理建立的 package-private 网关。
 *
 * <p>{@link DataFileHandle} 负责文件 identity、锁序和 {@code currentSizeInPages} 发布；调用本端口时，
 * 新建文件尚未发布，或已有文件已持 lifecycle/file-size 锁。本接口只处理给定 {@link FileChannel}
 * 的左闭右开页范围，不回调 Buffer Pool、registry、redo、FSP 或 flush。</p>
 *
 * <p>成功表示范围可以按页读取且新字节具有确定初始化内容，但不表示数据已经
 * {@link FileChannel#force(boolean) force} 到稳定介质；durability 由上层 flush/lifecycle 协议负责。
 * 失败时调用方不得推进可见页数。</p>
 */
interface DataFileGateway {

    /**
     * 在新建 channel 上建立初始页范围。
     *
     * <p>调用发生在 {@link DataFileHandle} 发布之前。实现必须覆盖
     * {@code [fromPageInclusive,toPageExclusive)} 的完整物理字节范围，使成功返回后的每页都能读取
     * 确定内容；空范围允许作为 no-op。</p>
     *
     * @param channel 已打开、由调用方拥有且可写的新 data-file channel
     * @param fromPageInclusive 非负起始页号，包含在初始化范围内
     * @param toPageExclusive 非负 exclusive 结束页号，必须不小于起点
     * @param pageSize 计算页到字节偏移的非空固定页大小
     * @param path channel 对应的非空诊断路径，不用于重新打开文件
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException channel、范围、PageSize 或 path
     *         非法时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 范围建立发生 IO 失败时抛出；
     *         调用方不得发布新 handle
     */
    void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive, PageSize pageSize, Path path);

    /**
     * 为已存在文件建立新增尾部页范围。
     *
     * <p>调用方在成功返回后才可推进 {@code currentSizeInPages}。实现必须对重复覆盖已存在范围保持
     * 物理安全，但该接口不承诺保留范围内旧内容；生产调用只传文件尾新增区间。</p>
     *
     * @param channel 已打开、由调用方持锁保护且可写的 data-file channel
     * @param fromPageInclusive 新增范围的非负包含起点，生产值为加锁后旧文件页数
     * @param toPageExclusive 新增范围的非负 exclusive 终点，必须不小于起点
     * @param pageSize 计算物理字节范围的非空固定页大小
     * @param path channel 对应的非空诊断路径
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException channel、范围、PageSize 或 path
     *         非法时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 分配/初始化范围发生 IO 失败时抛出；
     *         调用方必须保留旧可见页数
     */
    void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                         PageSize pageSize, Path path);
}
