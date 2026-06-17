package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

/**
 * 物理页统一头（设计 §5.3，纯值对象，不 import PageGuard）。checksum 不入本对象（派生值由 {@link PageChecksum} 盖）；
 * PageGuard 读写下沉到访问器 {@link PageEnvelope}。prev/next 无邻居用 FIL_NULL。
 *
 * @param spaceId    表空间。
 * @param pageNo     本页页号。
 * @param prevPageNo 前驱页号（leaf 兄弟链）；无则 FIL_NULL。
 * @param nextPageNo 后继页号；无则 FIL_NULL。
 * @param pageLsn    页 LSN。
 * @param pageType   页类型。
 */
public record FilePageHeader(SpaceId spaceId, long pageNo, long prevPageNo, long nextPageNo,
                             long pageLsn, PageType pageType) {

    /** 无邻居哨兵（InnoDB FIL_NULL = 0xFFFFFFFF）。 */
    public static final long FIL_NULL = 0xFFFFFFFFL;

    public FilePageHeader {
        if (spaceId == null || pageType == null) {
            throw new DatabaseValidationException("file page header spaceId/pageType must not be null");
        }
        if (pageNo < 0) {
            throw new DatabaseValidationException("page no must be non-negative: " + pageNo);
        }
        if (prevPageNo < 0 || nextPageNo < 0) {
            throw new DatabaseValidationException("prev/next page no must be non-negative or FIL_NULL");
        }
        if (pageLsn < 0) {
            throw new DatabaseValidationException("page lsn must be non-negative: " + pageLsn);
        }
    }
}
