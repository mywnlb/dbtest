package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;

/**
 * MySQL 8.0 file-per-table / general tablespace 默认自动扩展策略（设计 §8）：
 * 当前大小小于 1 个 extent 时逐页扩展；介于 1 与 32 个 extent 之间每次 1 个 extent；
 * 达到或超过 32 个 extent 后每次 4 个 extent，以提升大文件顺序性。
 *
 * <p>extent 页数复用 {@link PageSize#pagesPerExtent()}（4KB→256、8KB→128、16KB/32KB/64KB→64），
 * 不在此写死 64（设计 §6.1）。
 *
 * <p>简化点：Configured/Undo/FixedSize 等其它策略后续单独实现；AUTOEXTEND_SIZE 暂不支持。
 */
public final class DefaultIbdAutoExtendPolicy implements AutoExtendPolicy {

    /**
     * 切换到"每次 4 个 extent"所需的 extent 个数阈值（含）。
     */
    private static final long FOUR_EXTENT_THRESHOLD = 32L;

    @Override
    public long nextIncrementPages(long currentSizeInPages, PageSize pageSize) {
        if (currentSizeInPages < 0) {
            throw new DatabaseValidationException("current size must be non-negative: " + currentSizeInPages);
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
        long pagesPerExtent = pageSize.pagesPerExtent();
        if (currentSizeInPages < pagesPerExtent) {
            return 1L;
        }
        if (currentSizeInPages < FOUR_EXTENT_THRESHOLD * pagesPerExtent) {
            return pagesPerExtent;
        }
        return 4L * pagesPerExtent;
    }
}
