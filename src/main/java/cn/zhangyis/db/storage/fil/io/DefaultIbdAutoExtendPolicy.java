package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;

/**
 * 按设计文档中 MySQL 8.0 file-per-table/general 默认规则实现的分段自动扩展策略：
 * 当前大小小于 1 个 extent 时逐页扩展；介于 1 与 32 个 extent 之间每次 1 个 extent；
 * 达到或超过 32 个 extent 后每次 4 个 extent，以提升大文件顺序性。
 *
 * <p>extent 页数复用 {@link PageSize#pagesPerExtent()}（4KB→256、8KB→128、16KB/32KB/64KB→64），
 * 不把 16 KiB 页的 64 pages/extent 错用于其它页大小。</p>
 *
 * <p>当前 {@link FileChannelPageStore} 不读取 {@code TablespaceType}，默认对它管理的所有数据文件使用
 * 本策略，包括当前 UNDO 文件；尚未实现按空间类型、显式 AUTOEXTEND_SIZE 或固定容量选择不同策略。
 * 该差异只影响单次物理增量，FSP 是否可分配新增页仍由上层 page0 元数据控制。</p>
 */
public final class DefaultIbdAutoExtendPolicy implements AutoExtendPolicy {

    /**
     * 切换为每次四个 extent 的当前大小阈值，单位为 extent 数且包含边界值。
     */
    private static final long FOUR_EXTENT_THRESHOLD = 32L;

    /**
     * 根据当前页数所在区间返回 1 page、1 extent 或 4 extents。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝负当前大小和空 PageSize，避免用无效几何计算扩展边界。</li>
     *     <li>从 PageSize 取得 pages-per-extent；当前大小严格小于一个 extent 时只返回一页。</li>
     *     <li>当前大小位于一个 extent（含）到 32 extents（不含）时返回一个 extent。</li>
     *     <li>达到 32 extents 的边界及以上返回四个 extents；方法不修改文件或任何共享状态。</li>
     * </ol>
     *
     * @param currentSizeInPages 锁内观察到的非负当前物理页数
     * @param pageSize 非空文件页大小，用于推导 extent 几何
     * @return 严格为正的扩展页数，取值为 1、pages-per-extent 或其四倍
     * @throws DatabaseValidationException 当前大小为负或 pageSize 为空时抛出
     */
    @Override
    public long nextIncrementPages(long currentSizeInPages, PageSize pageSize) {
        // 1. 先建立可解释的非负文件几何。
        if (currentSizeInPages < 0) {
            throw new DatabaseValidationException("current size must be non-negative: " + currentSizeInPages);
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }

        // 2. 小文件逐页增长，避免首次扩展立即分配整个 extent。
        long pagesPerExtent = pageSize.pagesPerExtent();
        if (currentSizeInPages < pagesPerExtent) {
            return 1L;
        }

        // 3. 中等文件按一个 extent 增长；32-extents 边界不属于本区间。
        if (currentSizeInPages < FOUR_EXTENT_THRESHOLD * pagesPerExtent) {
            return pagesPerExtent;
        }

        // 4. 大文件按四个 extents 增长；这里只返回决策，不执行文件尾 IO。
        return 4L * pagesPerExtent;
    }
}
