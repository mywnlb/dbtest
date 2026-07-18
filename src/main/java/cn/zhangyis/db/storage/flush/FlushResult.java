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

    /**
     * 根据调用参数创建或转换 {@code ok} 返回的 {@code FlushResult}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param pageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param status 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @return {@code ok} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static FlushResult ok(PageId pageId, Lsn pageLsn, FlushResultStatus status) {
        return new FlushResult(pageId, pageLsn, status, null);
    }

    /**
     * 根据调用参数创建或转换 {@code failed} 返回的 {@code FlushResult}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param pageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     * @return {@code failed} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static FlushResult failed(PageId pageId, Lsn pageLsn, DatabaseRuntimeException failure) {
        return new FlushResult(pageId, pageLsn, FlushResultStatus.FAILED, failure);
    }
}
