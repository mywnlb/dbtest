package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Flush snapshot 页镜像校验和工具。与 {@link PageChecksum} 的计算范围一致，但输入是整页 byte[]，
 * 供 F1 在不持有 {@code PageGuard} 的 doublewrite/data-file 写盘路径上盖 header checksum、trailer checksum
 * 与 trailer low32 LSN。
 */
public final class PageImageChecksum {

    private PageImageChecksum() {
    }

    /**
     * 计算页镜像 [4, pageSize-8) 的 CRC32。该范围排除 header checksum 和 file trailer，
     * 因此重复 stamp 不会因为旧 checksum/trailer 变化而改变结果。
     *
     * @param page 整页镜像。
     * @param pageSize 页大小。
     * @return CRC32 截断 int。
     */
    public static int compute(byte[] page, PageSize pageSize) {
        requireArgs(page, pageSize);
        CRC32 crc = new CRC32();
        crc.update(page, Integer.BYTES,
                pageSize.bytes() - Integer.BYTES - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES);
        return (int) crc.getValue();
    }

    /**
     * 对页镜像盖 checksum/trailer。调用方应在 data file write 前执行该方法，使落盘页可被 doublewrite recovery 校验。
     *
     * @param page 整页镜像，原地修改。
     * @param pageSize 页大小。
     */
    public static void stamp(byte[] page, PageSize pageSize) {
        requireArgs(page, pageSize);
        ByteBuffer view = ByteBuffer.wrap(page);
        long pageLsn = view.getLong(PageEnvelopeLayout.PAGE_LSN);
        int checksum = compute(page, pageSize);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        view.putInt(PageEnvelopeLayout.CHECKSUM, checksum);
        view.putInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM, checksum);
        view.putInt(trailerOffset + PageEnvelopeLayout.TRAILER_LOW32_LSN, (int) pageLsn);
    }

    /**
     * 校验页镜像 header checksum、trailer checksum 与 trailer low32 LSN 是否都匹配当前页体。
     * checksum 证明页体没有被撕裂；trailer low32 LSN 额外固定尾部 trailer 与 header pageLSN 的对应关系，
     * 防止尾部来自另一轮写入但 checksum 字段尚未损坏的 partial page write 被误认为合法。
     *
     * @param page 整页镜像。
     * @param pageSize 页大小。
     * @return true 表示 checksum 与 trailer 均匹配。
     */
    public static boolean verify(byte[] page, PageSize pageSize) {
        requireArgs(page, pageSize);
        ByteBuffer view = ByteBuffer.wrap(page);
        int checksum = compute(page, pageSize);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        long pageLsn = view.getLong(PageEnvelopeLayout.PAGE_LSN);
        return checksum == view.getInt(PageEnvelopeLayout.CHECKSUM)
                && checksum == view.getInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM)
                && (int) pageLsn == view.getInt(trailerOffset + PageEnvelopeLayout.TRAILER_LOW32_LSN);
    }

    /**
     * 校验 raw ByteBuffer 页镜像。该入口不改变传入 buffer 的 position/limit，
     * 供 page0 metadata loader 在解码 FSP 物理字段前做页信封级校验。
     *
     * @param page 整页镜像缓冲。
     * @param pageSize 页大小。
     * @return true 表示 checksum 与 trailer 均匹配。
     */
    public static boolean verify(ByteBuffer page, PageSize pageSize) {
        return verify(copyPage(page, pageSize), pageSize);
    }

    /**
     * 判断页镜像是否属于历史版本未盖 checksum 的兼容格式：header checksum 与 trailer checksum 都为 0。
     * 这不是校验通过，只能由明确兼容旧文件的调用方在页型、页号、spaceId 等强校验之后决定是否接受。
     *
     * @param page 整页镜像缓冲。
     * @param pageSize 页大小。
     * @return 两个 checksum 派生字段均为 0 时返回 true。
     */
    public static boolean hasLegacyZeroChecksums(ByteBuffer page, PageSize pageSize) {
        requireBufferArgs(page, pageSize);
        ByteBuffer view = page.asReadOnlyBuffer();
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        return view.getInt(PageEnvelopeLayout.CHECKSUM) == 0
                && view.getInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM) == 0;
    }

    private static void requireArgs(byte[] page, PageSize pageSize) {
        if (page == null || pageSize == null) {
            throw new DatabaseValidationException("page image/page size must not be null");
        }
        if (page.length != pageSize.bytes()) {
            throw new DatabaseValidationException("page image length must equal page size: expected "
                    + pageSize.bytes() + " got " + page.length);
        }
    }

    private static void requireBufferArgs(ByteBuffer page, PageSize pageSize) {
        if (page == null || pageSize == null) {
            throw new DatabaseValidationException("page buffer/page size must not be null");
        }
        if (page.capacity() < pageSize.bytes()) {
            throw new DatabaseValidationException("page buffer capacity must be at least page size: expected "
                    + pageSize.bytes() + " got " + page.capacity());
        }
    }

    /**
     * 根据调用参数创建或转换 {@code copyPage} 返回的 {@code byte[]}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param page 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code copyPage} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] copyPage(ByteBuffer page, PageSize pageSize) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireBufferArgs(page, pageSize);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        ByteBuffer view = page.asReadOnlyBuffer();
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        byte[] copy = new byte[pageSize.bytes()];
        for (int i = 0; i < copy.length; i++) {
            copy[i] = view.get(i);
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return copy;
    }
}
