package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

/**
 * 一次 linear read-ahead 预取请求：表达「预取某表空间一个 extent 的连续页」。由 {@link LinearReadAheadTracker} 在
 * 检测到顺序访问达阈值时产出，{@code ReadAheadService} 的后台 worker 把它展开为对每个页的 {@code BufferPool.prefetch}。
 *
 * @param spaceId     目标表空间。
 * @param firstPageNo extent 起始页号（含）。
 * @param pageCount   连续预取页数（一个 extent 的页数）。
 */
public record ReadAheadRequest(SpaceId spaceId, long firstPageNo, int pageCount) {

    public ReadAheadRequest {
        if (spaceId == null) {
            throw new DatabaseValidationException("read-ahead spaceId must not be null");
        }
        if (firstPageNo < 0) {
            throw new DatabaseValidationException("read-ahead firstPageNo must be >= 0: " + firstPageNo);
        }
        if (pageCount < 1) {
            throw new DatabaseValidationException("read-ahead pageCount must be >= 1: " + pageCount);
        }
    }
}
