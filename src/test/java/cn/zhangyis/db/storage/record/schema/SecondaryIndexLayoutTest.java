package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 二级索引紧凑 entry 的字段投影、物理 key 和回表主键契约测试；只验证纯 metadata/value 行为，
 * 不打开 B+Tree 页或事务。
 */
class SecondaryIndexLayoutTest {

    /**
     * 二级 key 已含主键列时仍追加完整主键，避免物理格式依赖“是否重复”的隐式规则。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>构造包含重复主键来源、DESC 与 prefix 的 logical secondary key。</li>
     *     <li>创建紧凑 layout，验证 source ordinal、part 数、方向与 prefix 均按固定物理规则保留。</li>
     * </ol>
     */
    @Test
    void appendsCompleteClusterKeyEvenWhenLogicalKeyAlreadyContainsIt() {
        // 1. logical key 的第二个 part 已是主键列，物理布局仍必须在尾部再次追加完整主键。
        TableSchema table = tableSchema();
        IndexKeyDef secondary = new IndexKeyDef(20, List.of(
                new KeyPartDef(new ColumnId(1), KeyOrder.DESC, 3),
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        IndexKeyDef primary = new IndexKeyDef(10, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));

        // 2. 断言紧凑 schema 与物理 key definition 不采用隐式去重，并保留 logical part 比较属性。
        SecondaryIndexLayout layout = SecondaryIndexLayout.create(table, secondary, primary);

        assertFalse(layout.entrySchema().clustered());
        assertEquals(3, layout.entrySchema().columnCount());
        assertEquals(List.of(1, 0, 0), layout.sourceOrdinals());
        assertEquals(2, layout.logicalKeyPartCount());
        assertEquals(1, layout.clusterKeyPartCount());
        assertEquals(KeyOrder.DESC, layout.physicalKeyDef().parts().getFirst().order());
        assertEquals(3, layout.physicalKeyDef().parts().getFirst().prefixBytes());
        assertEquals(0, layout.physicalKeyDef().parts().getLast().prefixBytes());
    }

    /**
     * 全行投影只携带索引字段，entry 可反向提取完整聚簇主键。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建单列 logical secondary + 单列聚簇主键布局和完整表行。</li>
     *     <li>投影紧凑 entry，分别提取 logical、cluster 与完整 physical key 并核对字段边界。</li>
     * </ol>
     */
    @Test
    void projectsCompactEntryAndExtractsClusterKey() {
        // 1. 完整表行包含非索引 payload，二级 entry 只应保留 email 与主键后缀。
        SecondaryIndexLayout layout = SecondaryIndexLayout.create(tableSchema(),
                new IndexKeyDef(20, List.of(new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0))),
                new IndexKeyDef(10, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0))));
        LogicalRecord row = new LogicalRecord(7,
                List.of(new ColumnValue.IntValue(42), new ColumnValue.StringValue("alpha"),
                        new ColumnValue.StringValue("payload")),
                false, RecordType.CONVENTIONAL, null);

        // 2. 三种 key view 必须由同一紧凑 entry 切分，不能重新读取完整表行猜测。
        LogicalRecord entry = layout.toEntry(row, false);

        assertEquals(List.of(new ColumnValue.StringValue("alpha"), new ColumnValue.IntValue(42)),
                entry.columnValues());
        assertEquals(List.of(new ColumnValue.StringValue("alpha")), layout.logicalKey(entry).values());
        assertEquals(List.of(new ColumnValue.IntValue(42)), layout.clusterKey(entry).values());
        assertEquals(List.of(new ColumnValue.StringValue("alpha"), new ColumnValue.IntValue(42)),
                layout.physicalKey(entry).values());
    }

    /**
     * LOB/JSON envelope 不能成为 v1 二级 key，避免比较物理引用而非逻辑值。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>构造把 TEXT overflow-capable 列声明为 secondary part 的表 metadata。</li>
     *     <li>断言 layout 在 metadata 阶段抛领域校验异常，而非延迟到页比较或唯一检查。</li>
     * </ol>
     */
    @Test
    void rejectsOverflowCapableSecondaryKey() {
        // 1. TEXT 的页内值可能是 ExternalValue envelope，v1 不具备按逻辑全文比较的稳定协议。
        TableSchema table = new TableSchema(7, List.of(
                column(0, "id", ColumnType.bigint(false, false)),
                column(1, "body", ColumnType.text(true))), true);
        IndexKeyDef secondary = new IndexKeyDef(20, List.of(
                new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 8)));
        IndexKeyDef primary = new IndexKeyDef(10, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));

        // 2. 必须在 layout 创建时 fail-closed，禁止把物理 LOB reference 当作索引值。
        assertThrows(DatabaseValidationException.class,
                () -> SecondaryIndexLayout.create(table, secondary, primary));
    }

    /**
     * 构造测试共享的 exact-version 完整聚簇表 schema。
     *
     * @return schema version 7，含 BIGINT 主键、VARCHAR secondary 候选和非索引 payload 的聚簇 schema。
     */
    private static TableSchema tableSchema() {
        return new TableSchema(7, List.of(
                column(0, "id", ColumnType.bigint(false, false)),
                column(1, "code", ColumnType.varchar(64, false)),
                column(2, "payload", ColumnType.varchar(128, true))), true);
    }

    /**
     * 构造 column id 与 ordinal 一致的测试列定义。
     *
     * @param ordinal 连续且从零开始的 schema ordinal，同时作为 ColumnId。
     * @param name    便于失败诊断的列名。
     * @param type    当前列稳定类型与 nullable/collation 配置。
     * @return 可直接放入 {@link TableSchema} 的列定义。
     */
    private static ColumnDef column(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }
}
