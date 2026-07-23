package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 单目标页局部 merge 后的稳定统计。
 *
 * @param consumedOperations 已按序成功解释并达到幂等终态的 mutation 数
 * @param changedOperations 实际写过目标页的 mutation 数
 * @param freeBytes merge 后 RecordPage 连续空闲字节
 * @param freeSpaceClass 应写回 bitmap 的保守两位等级
 */
public record ChangeBufferMergeResult(int consumedOperations, int changedOperations,
                                      int freeBytes, int freeSpaceClass) {
    public ChangeBufferMergeResult {
        if (consumedOperations < 0 || changedOperations < 0 || changedOperations > consumedOperations
                || freeBytes < 0 || freeSpaceClass < 0 || freeSpaceClass > 3) {
            throw new DatabaseValidationException("invalid change buffer merge result");
        }
    }
}
