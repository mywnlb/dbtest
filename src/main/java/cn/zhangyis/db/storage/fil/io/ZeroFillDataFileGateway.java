package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 默认 data-file 网关：用 positional write 对目标页范围逐页写零。该实现跨平台、内容确定，
 * 是生产默认路径；平台 native preallocation 只能作为外层 adapter 的优化，不能替代零填充语义。
 */
final class ZeroFillDataFileGateway implements DataFileGateway {

    @Override
    public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                           PageSize pageSize, Path path) {
        zeroFill(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    @Override
    public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                PageSize pageSize, Path path) {
        zeroFill(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    private static void zeroFill(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                 PageSize pageSize, Path path) {
        validate(channel, fromPageInclusive, toPageExclusive, pageSize, path);
        ByteBuffer zero = ByteBuffer.allocate(pageSize.bytes());
        try {
            for (long page = fromPageInclusive; page < toPageExclusive; page++) {
                zero.clear();
                long pos = pageOffset(page, pageSize);
                while (zero.hasRemaining()) {
                    pos += channel.write(zero, pos);
                }
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("zero-fill failed for " + path
                    + " pages [" + fromPageInclusive + "," + toPageExclusive + ")", e);
        }
    }

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

    private static long pageOffset(long page, PageSize pageSize) {
        try {
            return Math.multiplyExact(page, (long) pageSize.bytes());
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("data file page offset overflow: page=" + page, overflow);
        }
    }
}
