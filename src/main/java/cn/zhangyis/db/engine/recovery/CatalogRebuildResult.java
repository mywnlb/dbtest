package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;

import java.nio.file.Path;

/**
 * catalog baseline 已验证并原子发布后的离线 rebuild 结果。
 *
 * @param catalogPath 最终发布的 {@code mysql.ibd}
 * @param dictionaryVersion baseline published version
 * @param schemaCount 重建 schema 数量
 * @param tableCount 重建 ACTIVE/DISCARDED 表数量
 * @param controlSnapshot 已 durable 的安全 identity 高水位
 * @param manifestDigest 授权本次重建的 clean manifest 摘要
 */
public record CatalogRebuildResult(
        Path catalogPath,
        DictionaryVersion dictionaryVersion,
        int schemaCount,
        int tableCount,
        DictionaryControlSnapshot controlSnapshot,
        String manifestDigest) {

    public CatalogRebuildResult {
        if (catalogPath == null || dictionaryVersion == null || schemaCount < 0 || tableCount < 0
                || controlSnapshot == null || manifestDigest == null || manifestDigest.isBlank()) {
            throw new DatabaseValidationException("catalog rebuild result is invalid");
        }
        catalogPath = catalogPath.toAbsolutePath().normalize();
    }
}
