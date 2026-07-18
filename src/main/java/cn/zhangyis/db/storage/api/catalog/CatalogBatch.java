package cn.zhangyis.db.storage.api.catalog;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** 已通过 frame CRC、batch SHA-256 和 committed-length header 验证的不可变 catalog 批次。
 *
 * @param sequence catalog 批次的单调序号；必须非负，持久化时不得回退或与已发布批次重复
 * @param records 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
 */
public record CatalogBatch(long sequence, List<CatalogRecord> records) {
    public CatalogBatch {
        if (sequence <= 0 || records == null || records.isEmpty()) {
            throw new DatabaseValidationException("catalog batch sequence/records are invalid");
        }
        records = List.copyOf(records);
    }
}
