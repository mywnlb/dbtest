package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 把平台空间预留与确定性零填充组合起来的 data-file gateway。
 *
 * <p>每个范围严格先调用 {@link PreallocationStrategy}，成功后再委托
 * {@link ZeroFillDataFileGateway} 写零。native 预留只优化底层空间分配，不能替代零填充的文件长度/
 * 内容保证；任一步失败都会阻止 {@code DataFileHandle} 发布新页数。失败可能留下不可见的部分预留或
 * 文件尾字节，后续重试仍从旧可见页数重新建立范围。</p>
 *
 * <p>当前 {@link FileChannelPageStore} 默认直接使用零填充 gateway，生产组合根没有注入本 adapter；
 * 本类是 package-private 平台优化 seam，不读取 registry/FSP 状态。</p>
 */
final class PreallocatingDataFileGateway implements DataFileGateway {

    /**
     * 在零填充之前执行的非空平台预留策略；构造后不可替换。
     */
    private final PreallocationStrategy strategy;

    /**
     * 建立确定文件长度与零内容的固定后置 gateway；它是内容正确性的 owner，不能由策略替代。
     */
    private final DataFileGateway zeroFillGateway;

    /**
     * 创建“预留后零填充”的组合 gateway。
     *
     * @param strategy 非空平台空间预留策略，允许是 no-op
     * @throws DatabaseValidationException strategy 为空时抛出
     */
    PreallocatingDataFileGateway(PreallocationStrategy strategy) {
        if (strategy == null) {
            throw new DatabaseValidationException("preallocation strategy must not be null");
        }
        this.strategy = strategy;
        this.zeroFillGateway = new ZeroFillDataFileGateway();
    }

    /**
     * 为新文件先预留目标字节范围，再写零建立全部初始页。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验页范围并换算字节范围，调用平台策略；失败时不进入零填充。</li>
     *     <li>策略成功后委托零填充 gateway 重新校验并写完整范围；只有该阶段成功，调用方才能发布 handle。</li>
     * </ol>
     *
     * @param channel 已打开且尚未发布的新文件 channel
     * @param fromPageInclusive 非负包含起点
     * @param toPageExclusive 不小于起点的 exclusive 终点
     * @param pageSize 非空固定页大小
     * @param path 非空诊断路径
     * @throws DatabaseValidationException 参数或页到字节换算非法时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 预留或零填充 IO 失败时抛出
     */
    @Override
    public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                           PageSize pageSize, Path path) {
        // 1. 平台预留只建立物理空间机会，不发布长度或确定内容。
        preallocate(channel, fromPageInclusive, toPageExclusive, pageSize, path);

        // 2. 完整零填充成功后，DataFileHandle 才可发布初始页数。
        zeroFillGateway.initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    /**
     * 为已有文件尾范围先预留空间，再写零建立新增页。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验并换算范围，先调用平台预留策略；失败时调用方保留旧可见大小。</li>
     *     <li>预留成功后零填充新增尾部；调用方仅在该阶段也成功后推进
     *     {@code currentSizeInPages}，但仍需单独 force 才能声明 durable。</li>
     * </ol>
     *
     * @param channel 已打开且由调用方持锁保护的文件 channel
     * @param fromPageInclusive 新增尾部范围的非负包含起点
     * @param toPageExclusive 不小于起点的 exclusive 终点
     * @param pageSize 非空固定页大小
     * @param path 非空诊断路径
     * @throws DatabaseValidationException 参数或页到字节换算非法时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 预留或零填充 IO 失败时抛出
     */
    @Override
    public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                PageSize pageSize, Path path) {
        // 1. 先尝试平台空间预留；失败时保留调用方旧可见大小。
        preallocate(channel, fromPageInclusive, toPageExclusive, pageSize, path);

        // 2. 再零填充新增尾部；成功是发布新页数的前置条件，但尚不代表 force durable。
        zeroFillGateway.ensureAllocated(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    /**
     * 校验页范围、做溢出安全的字节换算并调用平台策略。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空 channel/PageSize/path 和反向、负页范围。</li>
     *     <li>分别把起点页数与范围页数乘 PageSize，任何 long 溢出都在策略调用前失败。</li>
     *     <li>把非负字节范围交给策略；本方法不执行零填充或 size 发布。</li>
     * </ol>
     *
     * @param channel 已打开 data-file channel
     * @param fromPageInclusive 非负页起点
     * @param toPageExclusive 不小于起点的 exclusive 页终点
     * @param pageSize 非空固定页大小
     * @param path 非空诊断路径
     * @throws DatabaseValidationException 参数非法或字节范围溢出时抛出
     */
    private void preallocate(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                             PageSize pageSize, Path path) {
        // 1. 完整物理范围必须能由同一 channel、PageSize 和诊断路径解释。
        if (channel == null || pageSize == null || path == null) {
            throw new DatabaseValidationException("preallocation channel/pageSize/path must not be null");
        }
        if (fromPageInclusive < 0 || toPageExclusive < fromPageInclusive) {
            throw new DatabaseValidationException("invalid preallocation page range: ["
                    + fromPageInclusive + "," + toPageExclusive + ")");
        }

        // 2. 两次乘法均 fail-closed，禁止溢出后把负偏移/长度交给平台 API。
        long offsetBytes = pageOffset(fromPageInclusive, pageSize);
        long lengthBytes = pageOffset(toPageExclusive - fromPageInclusive, pageSize);

        // 3. 策略只接收已校验字节范围，内容初始化随后由零填充 gateway 完成。
        strategy.preallocate(channel, offsetBytes, lengthBytes, path);
    }

    /**
     * 把页数安全换算为字节数。
     *
     * @param pages 已校验的非负页数或范围长度
     * @param pageSize 非空固定页大小
     * @return pages 与 pageSize.bytes 的精确乘积
     * @throws DatabaseValidationException 乘积超出 long 时抛出并保留算术根因
     */
    private static long pageOffset(long pages, PageSize pageSize) {
        try {
            return Math.multiplyExact(pages, (long) pageSize.bytes());
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("preallocation byte range overflow: pages=" + pages, overflow);
        }
    }
}
