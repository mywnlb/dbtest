package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

/**
 * 单页 flush 结果。失败结果保留领域异常，成功/跳过结果不携带 failure。
 *
 * @param pageId 目标页。
 * @param pageLsn 本轮 flush 观察到的页 LSN；未取得 snapshot 时可为 0 边界。
 * @param status flush 结果状态。
 * @param failure 失败原因，仅 status=FAILED 时存在。
 */
public record FlushResult(PageId pageId, Lsn pageLsn, FlushResultStatus status, DatabaseRuntimeException failure) {

    public FlushResult {
        if (pageId == null || pageLsn == null || status == null) {
            throw new DatabaseValidationException("flush result page/pageLsn/status must not be null");
        }
        if (status == FlushResultStatus.FAILED && failure == null) {
            throw new DatabaseValidationException("failed flush result must keep failure cause");
        }
        if (status != FlushResultStatus.FAILED && failure != null) {
            throw new DatabaseValidationException("non-failed flush result must not keep failure cause");
        }
    }

    public static FlushResult ok(PageId pageId, Lsn pageLsn, FlushResultStatus status) {
        return new FlushResult(pageId, pageLsn, status, null);
    }

    public static FlushResult failed(PageId pageId, Lsn pageLsn, DatabaseRuntimeException failure) {
        return new FlushResult(pageId, pageLsn, FlushResultStatus.FAILED, failure);
    }
}
