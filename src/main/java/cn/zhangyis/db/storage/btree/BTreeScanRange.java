package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;

/**
 * 单 leaf 页内扫描边界。B1/B2 要求 lower/upper 都存在，避免把当前能力误表述为完整跨页 range scan。
 *
 * @param lowerKey       下界 key。
 * @param lowerInclusive true 表示包含等于下界的记录。
 * @param upperKey       上界 key。
 * @param upperInclusive true 表示包含等于上界的记录。
 * @param limit          最多返回记录数；0 表示只校验索引和页，不返回记录。
 */
public record BTreeScanRange(SearchKey lowerKey, boolean lowerInclusive,
                             SearchKey upperKey, boolean upperInclusive, int limit) {

    public BTreeScanRange {
        if (lowerKey == null || upperKey == null) {
            throw new DatabaseValidationException("btree scan lower/upper keys must not be null");
        }
        if (limit < 0) {
            throw new DatabaseValidationException("btree scan limit must be non-negative: " + limit);
        }
    }
}
