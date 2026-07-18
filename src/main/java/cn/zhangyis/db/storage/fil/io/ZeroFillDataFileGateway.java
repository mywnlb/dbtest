package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 用 positional write 建立确定零内容页范围的默认 data-file gateway。
 *
 * <p>实现按页复用一个 PageSize 大小的零初始化 {@link ByteBuffer}，不依赖 channel 的共享 position。
 * 写到文件尾之外会同时建立物理长度；成功不执行 {@link FileChannel#force(boolean)}，因此只建立
 * 当前进程可见的完整范围，不声明 stable-media durability。</p>
 *
 * <p>生产 create/extend/ensure-capacity 只传尚未发布的文件尾范围。若错误地传入已有数据页，本实现会
 * 覆盖其内容；它不解析页类型、page LSN 或 redo。平台 preallocation 只能包在外层优化空间预留，
 * 不能替代本类的内容初始化。</p>
 */
final class ZeroFillDataFileGateway implements DataFileGateway {

    /**
     * 零填充新建文件的初始页范围。
     *
     * @param channel 已打开且尚未发布的可写 channel
     * @param fromPageInclusive 非负包含起点
     * @param toPageExclusive 不小于起点的 exclusive 终点
     * @param pageSize 非空固定页大小
     * @param path 非空诊断路径
     * @throws DatabaseValidationException 参数或页偏移换算非法时抛出
     * @throws DataFilePhysicalException positional write 失败时抛出；可能已写入部分不可见范围
     */
    @Override
    public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                           PageSize pageSize, Path path) {
        zeroFill(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    /**
     * 零填充已有文件的新增尾部页范围。
     *
     * @param channel 已打开且由调用方持 lifecycle/file-size 锁保护的可写 channel
     * @param fromPageInclusive 新增范围的非负包含起点
     * @param toPageExclusive 不小于起点的 exclusive 终点
     * @param pageSize 非空固定页大小
     * @param path 非空诊断路径
     * @throws DatabaseValidationException 参数或页偏移换算非法时抛出
     * @throws DataFilePhysicalException positional write 失败时抛出；调用方不得推进可见页数
     */
    @Override
    public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                PageSize pageSize, Path path) {
        zeroFill(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    /**
     * 校验并逐页写满目标左闭右开范围。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 channel、PageSize、path 和页范围，确保还未产生 IO。</li>
     *     <li>分配一个完整页的零 buffer；每页开始前 clear，保持相同零内容可重复使用。</li>
     *     <li>对每个页号做溢出安全的字节偏移换算，并循环 positional write 直到该页 buffer 消耗完。</li>
     *     <li>底层 IOException 包装为物理异常并保留范围上下文；此前成功页或当前部分页不会回滚，
     *     但 DataFileHandle 不会发布新 size。</li>
     * </ol>
     *
     * @param channel 目标可写 channel
     * @param fromPageInclusive 非负包含起点
     * @param toPageExclusive 不小于起点的 exclusive 终点
     * @param pageSize 非空固定页大小
     * @param path 非空诊断路径
     * @throws DatabaseValidationException 参数非法或任一页偏移乘法溢出时抛出
     * @throws DataFilePhysicalException 写页发生 IOException 时抛出并保留根因
     */
    private static void zeroFill(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                 PageSize pageSize, Path path) {
        // 1. 在首次写入之前完成公共参数和范围检查。
        validate(channel, fromPageInclusive, toPageExclusive, pageSize, path);

        // 2. Java 新分配 buffer 初始内容全零；每页 clear 只复位 position/limit，不改变零字节。
        ByteBuffer zero = ByteBuffer.allocate(pageSize.bytes());
        try {
            // 3. 每页使用独立物理偏移并写满整个 buffer，不依赖 channel position。
            for (long page = fromPageInclusive; page < toPageExclusive; page++) {
                zero.clear();
                long pos = pageOffset(page, pageSize);
                while (zero.hasRemaining()) {
                    pos += channel.write(zero, pos);
                }
            }
        } catch (IOException e) {
            // 4. 保留底层原因和完整页范围；调用方保持旧可见 size，后续可从旧边界重试。
            throw new DataFilePhysicalException("zero-fill failed for " + path
                    + " pages [" + fromPageInclusive + "," + toPageExclusive + ")", e);
        }
    }

    /**
     * 校验零填充所需的非空资源和有序非负页范围。
     *
     * @param channel 目标 channel
     * @param fromPageInclusive 包含起点
     * @param toPageExclusive exclusive 终点
     * @param pageSize 文件页大小
     * @param path 诊断路径
     * @throws DatabaseValidationException 任一必填引用为空、起点为负或终点小于起点时抛出
     */
    private static void validate(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                 PageSize pageSize, Path path) {
        if (channel == null || pageSize == null || path == null) {
            throw new DatabaseValidationException("data file gateway channel/pageSize/path must not be null");
        }
        if (fromPageInclusive < 0 || toPageExclusive < fromPageInclusive) {
            throw new DatabaseValidationException("invalid data file page range: ["
                    + fromPageInclusive + "," + toPageExclusive + ")");
        }
    }

    /**
     * 把单个页号安全换算为文件字节偏移。
     *
     * @param page 已校验的非负页号
     * @param pageSize 非空固定页大小
     * @return page 与 pageSize.bytes 的精确乘积
     * @throws DatabaseValidationException 乘积超过 long 范围时抛出并保留算术根因
     */
    private static long pageOffset(long page, PageSize pageSize) {
        try {
            return Math.multiplyExact(page, (long) pageSize.bytes());
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("data file page offset overflow: page=" + page, overflow);
        }
    }
}
