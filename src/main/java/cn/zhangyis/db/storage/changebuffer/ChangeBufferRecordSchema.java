package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

import java.util.List;

/** 全局 Change Buffer B+Tree 的内部非聚簇 schema/key 定义。 */
public final class ChangeBufferRecordSchema {

    /** 持久内部索引 id；ASCII `IBUF`，与用户 DD id 空间隔离。 */
    public static final long INDEX_ID = 0x49425546L;
    /** 内部 record schema 版本；不跟随用户 DD schema。 */
    public static final long SCHEMA_VERSION = 1L;

    private ChangeBufferRecordSchema() {
    }

    /**
     * 构造目标页 identity 三列加完整 mutation payload 的内部 schema。
     *
     * @param pageSize 实例页大小，用于限制 payload VARBINARY
     * @return 非聚簇四列 schema
     */
    public static TableSchema schema(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("change buffer record schema page size must not be null");
        }
        return new TableSchema(SCHEMA_VERSION, List.of(
                column(0, "target_space", ColumnType.bigint(true, false)),
                column(1, "target_page", ColumnType.bigint(true, false)),
                column(2, "sequence", ColumnType.bigint(true, false)),
                column(3, "mutation", ColumnType.varbinary(pageSize.bytes() - 256, false))), false);
    }

    /** @return 覆盖前三列的升序物理唯一 key。 */
    public static IndexKeyDef keyDef() {
        return new IndexKeyDef(INDEX_ID, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(2), KeyOrder.ASC, 0)));
    }

    private static ColumnDef column(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }
}
