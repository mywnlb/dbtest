package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;

/** DD coordinator 与 recovery 共用的 schema digest 门面；不读取 cache、repository、SDI 或物理页面。 */
public final class DdlSchemaDigestService {

    /** 无状态 canonical encoder；实例不持共享缓冲区，可以被多个 DDL 并发调用。 */
    private final TableSchemaDigestCodec codec = new TableSchemaDigestCodec();

    /**
     * 对一个显式 schema image 计算 TABLE_SCHEMA_V1/SHA-256。
     *
     * @param schema table 所属的 exact schema 定义
     * @param table source、intermediate 或 target 的完整不可变聚合
     * @param rowFormatVersion 聚簇 record 的正稳定格式版本；不能用 table dictionary version代替
     * @return 防御性持有 32-byte 输出的稳定 digest
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException identity、集合上限或UTF-8不合法时抛出
     */
    public DdlSchemaDigest digest(SchemaDefinition schema, TableDefinition table,
                                  long rowFormatVersion) {
        return codec.digest(new DdlTableSchemaImage(schema, table, rowFormatVersion));
    }
}
