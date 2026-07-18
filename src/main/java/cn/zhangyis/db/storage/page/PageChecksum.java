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

    /** 计算页体 [4, pageSize-8) 的 CRC32（截断为 int）。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code compute} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    public static int compute(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        byte[] body = guard.readBytes(4, pageSize.bytes() - 4 - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES);
        CRC32 crc = new CRC32();
        crc.update(body);
        return (int) crc.getValue();
    }

    /** 封页（要求 X）：算 checksum 并写 header.checksum、trailer.checksumTrailer，同步 trailer.low32Lsn = pageLsn 低 32 位。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     */
    public static void stamp(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        long pageLsn = guard.readLong(PageEnvelopeLayout.PAGE_LSN);
        int checksum = compute(guard, pageSize);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        guard.writeInt(PageEnvelopeLayout.CHECKSUM, checksum);
        guard.writeInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM, checksum);
        guard.writeInt(trailerOffset + PageEnvelopeLayout.TRAILER_LOW32_LSN, (int) pageLsn);
    }

    /** 校验：重算 checksum，与 header.checksum 且 trailer.checksumTrailer 同时相等才返回 true。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code verify} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
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
