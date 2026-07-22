package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.EnumSet;
import java.util.Set;

/**
 * canonical digest 的显式输入。rowFormatVersion 独立于 dictionary version；逻辑 LOB capability 只由列类型推导，
 * 因而 planned CREATE/shadow target 不需要先伪造物理 binding。
 *
 * @param schema table 所属且在 initial/final MDL 下冻结的 schema 定义
 * @param table source、intermediate 或 target 的完整 table 聚合
 * @param rowFormatVersion record codec 使用的正稳定版本；metadata-only/index DDL 前后必须保持不变
 */
public record DdlTableSchemaImage(SchemaDefinition schema, TableDefinition table,
                                  long rowFormatVersion) {

    /** 会要求表级 LOB ownership 的逻辑类型集合；不能由物理 segment 是否已分配反推。 */
    private static final Set<DictionaryTypeId> LOB_TYPES = EnumSet.of(
            DictionaryTypeId.TINYTEXT, DictionaryTypeId.TEXT,
            DictionaryTypeId.MEDIUMTEXT, DictionaryTypeId.LONGTEXT,
            DictionaryTypeId.TINYBLOB, DictionaryTypeId.BLOB,
            DictionaryTypeId.MEDIUMBLOB, DictionaryTypeId.LONGBLOB,
            DictionaryTypeId.JSON);

    public DdlTableSchemaImage {
        if (schema == null || table == null || rowFormatVersion <= 0
                || !schema.id().equals(table.schemaId())) {
            throw new DatabaseValidationException(
                    "DDL schema image requires matching schema/table and positive row format version");
        }
    }

    /**
     * 从列类型推导目标 record 是否可能产生 external LOB reference。
     *
     * @return 任一列属于 TEXT/BLOB/JSON 家族时为 true，否则为 false
     */
    public boolean requiresLobCapability() {
        return table.columns().stream().anyMatch(column -> LOB_TYPES.contains(column.type().typeId()));
    }
}
