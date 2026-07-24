package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 最终物理结果边界。
 *
 * @param offset 有序输入前需要跳过的行数
 * @param count 最多发布的行数
 */
public record PhysicalLimit(long offset, long count) {

    public PhysicalLimit {
        if (offset < 0 || count < 0) {
            throw new DatabaseValidationException(
                    "physical LIMIT offset/count must be non-negative");
        }
    }

    /**
     * 返回 Top-N 必须保留的 offset+count 大小，并拒绝算术溢出。
     *
     * @return 非负保留行数
     * @throws DatabaseValidationException offset+count 超过 long 时抛出
     */
    public long retainedRows() {
        try {
            return Math.addExact(offset, count);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException(
                    "physical LIMIT offset plus count overflows", overflow);
        }
    }
}
