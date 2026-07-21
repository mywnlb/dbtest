package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.List;

/**
 * 索引连续前缀的一侧端点。keyValues 可以短于完整 physical key；B+Tree 比较器把相等前缀视为
 * 同一区间，从而自然覆盖 secondary clustered suffix，而不把伪 sentinel 编码进页。
 *
 * @param keyValues 按访问索引声明顺序排列的非空连续前缀
 * @param inclusive 是否包含与该前缀比较相等的全部物理 key
 */
public record BoundRangeEndpoint(List<SqlValue> keyValues, boolean inclusive) {
    public BoundRangeEndpoint {
        if (keyValues == null || keyValues.isEmpty()
                || keyValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("range endpoint requires non-empty typed key values");
        }
        keyValues = List.copyOf(keyValues);
    }
}
