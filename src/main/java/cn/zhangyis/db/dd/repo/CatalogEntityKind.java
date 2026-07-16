package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;

import java.util.Arrays;

/** DD logical row kind 的稳定磁盘码；不得依赖 enum ordinal。 */
enum CatalogEntityKind {
    SCHEMA(1),
    TABLE(2),
    COLUMN(3),
    INDEX(4),
    SCHEMA_TOMBSTONE(5),
    TABLE_TOMBSTONE(6),
    DDL_LOG(7),
    CATALOG_COMMIT(127);

    private final int stableCode;

    CatalogEntityKind(int stableCode) {
        this.stableCode = stableCode;
    }

    int stableCode() {
        return stableCode;
    }

    static CatalogEntityKind fromStableCode(int code) {
        return Arrays.stream(values()).filter(kind -> kind.stableCode == code).findFirst()
                .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                        "unknown dictionary catalog entity kind: " + code));
    }
}
