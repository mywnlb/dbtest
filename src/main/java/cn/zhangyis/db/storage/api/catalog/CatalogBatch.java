package cn.zhangyis.db.storage.api.catalog;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** 已通过 frame CRC、batch SHA-256 和 committed-length header 验证的不可变 catalog 批次。 */
public record CatalogBatch(long sequence, List<CatalogRecord> records) {
    public CatalogBatch {
        if (sequence <= 0 || records == null || records.isEmpty()) {
            throw new DatabaseValidationException("catalog batch sequence/records are invalid");
        }
        records = List.copyOf(records);
    }
}
