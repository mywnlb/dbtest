package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;

import java.util.List;

/**
 * 灾难恢复 manifest 与 catalog rebuild 共用的全量字典 archive 门面。
 *
 * <p>该类只暴露逻辑 records，不打开文件。真正写入 {@code mysql.ibd} 时仍必须通过
 * {@link cn.zhangyis.db.storage.api.catalog.InternalCatalogStore} 原子 append，并在发布前重新打开验证。</p>
 */
public final class DictionaryCatalogArchiveCodec {

    /** package 内稳定 codec；公开门面避免 recovery 包依赖其 mutation 内部类型。 */
    private final DictionaryCatalogCodec delegate = new DictionaryCatalogCodec();

    /**
     * 编码完整稳定快照。
     *
     * @param snapshot clean manifest 中的字典快照
     * @return 确定性 baseline records
     */
    public List<CatalogRecord> encode(DictionarySnapshot snapshot) {
        return delegate.encodeBaseline(snapshot);
    }

    /**
     * 解码并完整校验 baseline records。
     *
     * @param records 从 manifest archive 或新 catalog 首批读取的 records
     * @return 重建后的不可变字典快照
     */
    public DictionarySnapshot decode(List<CatalogRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new DatabaseValidationException("dictionary archive records must not be null or empty");
        }
        return delegate.decodeBaseline(new CatalogBatch(1, records))
                .orElseThrow(() -> new DatabaseValidationException(
                        "dictionary archive does not contain a baseline batch"));
    }
}
