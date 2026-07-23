package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * 访问索引的可选上下边界；空 Optional 分别表达负无穷和正无穷。
 *
 * @param lower 可选物理排序下界
 * @param upper 可选物理排序上界
 */
public record IndexRange(Optional<RangeEndpoint> lower, Optional<RangeEndpoint> upper) {
    /**
     * 冻结两侧 endpoint 容器；无界必须使用空 Optional 而不是 Java {@code null}。
     *
     * @throws DatabaseValidationException 任一 Optional 容器缺失时抛出
     */
    public IndexRange {
        if (lower == null || upper == null) {
            throw new DatabaseValidationException(
                    "physical index range optionals must not be null");
        }
    }

    /**
     * 创建覆盖整个访问索引的不可变范围。
     *
     * @return 两侧均无界的范围
     */
    public static IndexRange unbounded() {
        return new IndexRange(Optional.empty(), Optional.empty());
    }
}
