package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/** 一轮批量刷脏请求；source 决定候选排序语义。
 *
 * @param source 选择 {@code 构造} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param targetLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
 * @param maxPages 参与 {@code 构造} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
 */
public record FlushBatchRequest(FlushBatchSource source, Lsn targetLsn, int maxPages) {

    public FlushBatchRequest {
        if (source == null || targetLsn == null) {
            throw new DatabaseValidationException("flush batch source/target must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("flush batch max pages must not be negative: " + maxPages);
        }
    }
}
