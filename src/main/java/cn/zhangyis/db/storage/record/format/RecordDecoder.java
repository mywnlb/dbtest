package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * 物理记录字节 → 逻辑记录（innodb-record-design §6）。schemaVersion 不存于记录字节（简化），解码取所给 schema 版本；
 * 列布局完全由 schema + NULL 位图 + 变长目录推导。
 *
 * <p>布局推导委托 {@link RecordFieldResolver}（与 {@code RecordCursor} 共用单一布局真相），本类只是「整条物化」的薄入口。
 */
public final class RecordDecoder {

    private final RecordFieldResolver resolver;

    public RecordDecoder(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.resolver = new RecordFieldResolver(registry);
    }

    /** 解码整条记录字节为逻辑记录。 */
    public LogicalRecord decode(byte[] buf, TableSchema schema) {
        if (buf == null || schema == null) {
            throw new DatabaseValidationException("buf/schema must not be null");
        }
        return resolver.resolve(buf, schema).materialize();
    }
}
