package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/** Adaptive policy 对 FlushList/LRU 两种来源的页数分配。
 *
 * @param targetLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
 * @param flushListPages 参与 {@code 构造} 的上界或规格值 {@code flushListPages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param lruPages 参与 {@code 构造} 的上界或规格值 {@code lruPages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param synchronousPressure 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
 */
public record FlushBatchPlan(Lsn targetLsn, int flushListPages, int lruPages, boolean synchronousPressure) {
    public FlushBatchPlan {
        if (targetLsn == null || flushListPages < 0 || lruPages < 0) {
            throw new DatabaseValidationException("invalid flush batch plan");
        }
    }

    public int totalPages() {
        return flushListPages + lruPages;
    }
}
