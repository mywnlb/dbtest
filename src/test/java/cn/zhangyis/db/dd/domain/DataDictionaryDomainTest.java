package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data Dictionary 领域对象第一轮 TDD：先锁定名称、稳定身份和表聚合不变量，避免后续 catalog codec
 * 或 DDL coordinator 各自复制一套不一致的校验规则。
 */
class DataDictionaryDomainTest {

    /** Unicode 名称先做 NFC，再用 Locale.ROOT 生成大小写不敏感 canonical key。 */
    @Test
    void normalizesDictionaryObjectNamesWithoutLosingDisplayName() {
        ObjectName name = ObjectName.of("CAF\u0045\u0301");

        assertEquals("CAF\u0049\u0301".replace('I', 'E'), name.displayName());
        assertEquals("caf\u00e9", name.canonicalName());
        assertEquals(ObjectName.of("caf\u00c9"), name);
    }

    /** 名称边界必须在进入 MDL key、name directory 和磁盘 codec 前统一拒绝。 */
    @Test
    void rejectsBlankControlAndOverlongNames() {
        assertThrows(DatabaseValidationException.class, () -> ObjectName.of("  "));
        assertThrows(DatabaseValidationException.class, () -> ObjectName.of("bad\u0000name"));
        assertThrows(DatabaseValidationException.class, () -> ObjectName.of("x".repeat(65)));
    }

    /** DD id 永不使用 0，防止未初始化值被误当成已持久化对象。 */
    @Test
    void dictionaryIdentifiersArePositive() {
        assertEquals(7L, TableId.of(7).value());
        assertEquals(9L, IndexId.of(9).value());
        assertThrows(DatabaseValidationException.class, () -> SchemaId.of(0));
        assertThrows(DatabaseValidationException.class, () -> DictionaryVersion.of(-1));
    }

    /** 表聚合复制输入集合，并要求唯一聚簇主键引用真实列。 */
    @Test
    void tableDefinitionOwnsColumnsAndOneClusteredPrimaryIndex() {
        List<ColumnDefinition> mutableColumns = new ArrayList<>();
        mutableColumns.add(new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.integer(false, false), 0));
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));

        TableDefinition table = new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("Orders"),
                DictionaryVersion.of(4), TableState.ACTIVE, mutableColumns, List.of(primary));
        mutableColumns.clear();

        assertEquals("orders", table.name().canonicalName());
        assertEquals(1, table.columns().size());
        assertTrue(table.primaryIndex().clustered());
        assertThrows(UnsupportedOperationException.class, () -> table.columns().clear());
    }

    /** ordinal 空洞、重复聚簇索引和引用缺失列会破坏 record layout，必须在构造时失败。 */
    @Test
    void rejectsInvalidTableAggregate() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 1);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(99, IndexOrder.ASC, 0)));

        assertThrows(DatabaseValidationException.class, () -> new TableDefinition(TableId.of(2), SchemaId.of(1),
                ObjectName.of("t"), DictionaryVersion.of(1), TableState.ACTIVE, List.of(id), List.of(primary)));
    }

    /** index id 是 undo/catalog 定位权威，即使名称不同也不能在同一 table 聚合中重复。 */
    @Test
    void rejectsDuplicateLogicalIndexIdentity() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        IndexDefinition duplicate = new IndexDefinition(IndexId.of(3), ObjectName.of("idx_id"), false, false,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));

        assertThrows(DatabaseValidationException.class, () -> new TableDefinition(TableId.of(2), SchemaId.of(1),
                ObjectName.of("t"), DictionaryVersion.of(1), TableState.ACTIVE,
                List.of(id), List.of(primary, duplicate)));
    }
}
