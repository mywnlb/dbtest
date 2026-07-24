package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexOrder;

/**
 * 已解析到 exact table column identity 的排序键。
 *
 * @param columnOrdinal exact TableDefinition 中的列位置
 * @param columnId 对应列的稳定 DD 身份，用于阻止跨版本 ordinal 误绑定
 * @param direction SQL 请求的升降序
 */
public record BoundSortKey(
        int columnOrdinal, long columnId, IndexOrder direction) {

    public BoundSortKey {
        if (columnOrdinal < 0 || columnId <= 0 || direction == null) {
            throw new DatabaseValidationException("invalid bound sort key");
        }
    }
}
