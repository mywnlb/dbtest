package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 预分配 adapter：先调用平台策略，再用零填充网关保证新增页内容确定。生产默认不走该 adapter；
 * 它只作为后续 Linux/Windows 平台优化注入点，保持 PageStore registry-free。
 */
final class PreallocatingDataFileGateway implements DataFileGateway {

    private final PreallocationStrategy strategy;
    private final DataFileGateway zeroFillGateway;

    PreallocatingDataFileGateway(PreallocationStrategy strategy) {
        if (strategy == null) {
            throw new DatabaseValidationException("preallocation strategy must not be null");
        }
        this.strategy = strategy;
        this.zeroFillGateway = new ZeroFillDataFileGateway();
    }

    @Override
    public void initialize(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                           PageSize pageSize, Path path) {
        preallocate(channel, fromPageInclusive, toPageExclusive, pageSize, path);
        zeroFillGateway.initialize(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    @Override
    public void ensureAllocated(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                                PageSize pageSize, Path path) {
        preallocate(channel, fromPageInclusive, toPageExclusive, pageSize, path);
        zeroFillGateway.ensureAllocated(channel, fromPageInclusive, toPageExclusive, pageSize, path);
    }

    private void preallocate(FileChannel channel, long fromPageInclusive, long toPageExclusive,
                             PageSize pageSize, Path path) {
        if (channel == null || pageSize == null || path == null) {
            throw new DatabaseValidationException("preallocation channel/pageSize/path must not be null");
        }
        if (fromPageInclusive < 0 || toPageExclusive < fromPageInclusive) {
            throw new DatabaseValidationException("invalid preallocation page range: ["
                    + fromPageInclusive + "," + toPageExclusive + ")");
        }
        long offsetBytes = pageOffset(fromPageInclusive, pageSize);
        long lengthBytes = pageOffset(toPageExclusive - fromPageInclusive, pageSize);
        strategy.preallocate(channel, offsetBytes, lengthBytes, path);
    }

    private static long pageOffset(long pages, PageSize pageSize) {
        try {
            return Math.multiplyExact(pages, (long) pageSize.bytes());
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("preallocation byte range overflow: pages=" + pages, overflow);
        }
    }
}
