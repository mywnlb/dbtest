package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 物理索引连续前缀的一侧端点。短 key 表示前缀边界，可覆盖 secondary key 后追加的聚簇后缀。
 *
 * @param keyValues 按访问索引声明顺序排列的非空、非 SQL NULL 连续前缀
 * @param inclusive 是否包含与该前缀相等的全部物理 key
 */
public record RangeEndpoint(List<SqlValue> keyValues, boolean inclusive) {
    /**
     * 校验并冻结可编码的连续索引前缀。
     *
     * @throws DatabaseValidationException key 容器为空、含 Java {@code null} 或 SQL NULL 时抛出
     */
    public RangeEndpoint {
        if (keyValues == null || keyValues.isEmpty()
                || keyValues.stream().anyMatch(java.util.Objects::isNull)
                || keyValues.stream().anyMatch(SqlValue.NullValue.class::isInstance)) {
            throw new DatabaseValidationException(
                    "physical range endpoint requires non-empty non-NULL typed key values");
        }
        keyValues = List.copyOf(keyValues);
    }
}
