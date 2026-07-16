package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** CREATE TABLE 的索引物理请求；table 请求保证恰好一个 clustered，其余为 secondary 索引。 */
public record StorageIndexDefinition(long indexId, String name, boolean unique, boolean clustered,
                                     List<StorageIndexKeyPart> keyParts) {
    public StorageIndexDefinition {
        if (indexId <= 0 || name == null || name.isBlank() || keyParts == null || keyParts.isEmpty()) {
            throw new DatabaseValidationException("invalid storage index definition");
        }
        keyParts = List.copyOf(keyParts);
        if (clustered && !unique) {
            throw new DatabaseValidationException("clustered storage index must be unique");
        }
    }
}
