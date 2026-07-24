package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexOrder;

/**
 * Executor 可直接消费的 table-column 排序键。
 *
 * @param columnOrdinal SortNode 输入完整 table schema 中的位置
 * @param direction SQL 请求的方向
 */
public record PhysicalSortKey(int columnOrdinal, IndexOrder direction) {

    public PhysicalSortKey {
        if (columnOrdinal < 0 || direction == null) {
            throw new DatabaseValidationException("invalid physical sort key");
        }
    }
}
