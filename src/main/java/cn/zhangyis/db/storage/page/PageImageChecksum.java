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

    private static byte[] copyPage(ByteBuffer page, PageSize pageSize) {
        requireBufferArgs(page, pageSize);
        ByteBuffer view = page.asReadOnlyBuffer();
        byte[] copy = new byte[pageSize.bytes()];
        for (int i = 0; i < copy.length; i++) {
            copy[i] = view.get(i);
        }
        return copy;
    }
}
