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
     * 校验页镜像 header checksum 与 trailer checksum 是否都等于当前页体 CRC32。
     *
     * @param page 整页镜像。
     * @param pageSize 页大小。
     * @return true 表示 checksum 匹配。
     */
    public static boolean verify(byte[] page, PageSize pageSize) {
        requireArgs(page, pageSize);
        ByteBuffer view = ByteBuffer.wrap(page);
        int checksum = compute(page, pageSize);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        return checksum == view.getInt(PageEnvelopeLayout.CHECKSUM)
                && checksum == view.getInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM);
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
}
