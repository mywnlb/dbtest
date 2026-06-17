package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;

import java.util.zip.CRC32;

/**
 * 页校验和（设计 §5.3/§14，CRC32 简化实现）。计算范围 [4, pageSize-8)，排除 4 字节头 checksum 与 8 字节 trailer。
 * 简化点：单一 CRC32、未抽 ChecksumStrategy（待 flush 切片）。
 */
public final class PageChecksum {

    private PageChecksum() {
    }

    /** 计算页体 [4, pageSize-8) 的 CRC32（截断为 int）。 */
    public static int compute(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        byte[] body = guard.readBytes(4, pageSize.bytes() - 4 - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES);
        CRC32 crc = new CRC32();
        crc.update(body);
        return (int) crc.getValue();
    }

    /** 封页（要求 X）：算 checksum 并写 header.checksum、trailer.checksumTrailer，同步 trailer.low32Lsn = pageLsn 低 32 位。 */
    public static void stamp(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        long pageLsn = guard.readLong(PageEnvelopeLayout.PAGE_LSN);
        int checksum = compute(guard, pageSize);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        guard.writeInt(PageEnvelopeLayout.CHECKSUM, checksum);
        guard.writeInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM, checksum);
        guard.writeInt(trailerOffset + PageEnvelopeLayout.TRAILER_LOW32_LSN, (int) pageLsn);
    }

    /** 校验：重算 checksum，与 header.checksum 且 trailer.checksumTrailer 同时相等才返回 true。 */
    public static boolean verify(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        int checksum = compute(guard, pageSize);
        int header = guard.readInt(PageEnvelopeLayout.CHECKSUM);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        int trailer = guard.readInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM);
        return checksum == header && checksum == trailer;
    }

    private static void requireArgs(PageGuard guard, PageSize pageSize) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
    }
}
