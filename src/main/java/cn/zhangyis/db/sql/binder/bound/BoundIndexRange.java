package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * 访问索引的可选上下边界；empty Optional 明确表示负/正无穷，不使用 Java null。
 *
 * @param lower 可选物理排序下界
 * @param upper 可选物理排序上界
 */
public record BoundIndexRange(Optional<BoundRangeEndpoint> lower,
                              Optional<BoundRangeEndpoint> upper) {
    public BoundIndexRange {
        if (lower == null || upper == null) {
            throw new DatabaseValidationException("bound index range optionals must not be null");
        }
    }

    /**
     * 创建覆盖整个索引的范围。
     *
     * @return 两侧均无界的不可变范围
     */
    public static BoundIndexRange unbounded() {
        return new BoundIndexRange(Optional.empty(), Optional.empty());
    }
}
