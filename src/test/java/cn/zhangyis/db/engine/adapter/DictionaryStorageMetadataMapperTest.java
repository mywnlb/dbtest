package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.adapter.exception.DictionaryStorageMappingException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DD immutable aggregate 到 storage metadata 快照的纯映射 TDD。 */
class DictionaryStorageMetadataMapperTest {

    @TempDir
    Path directory;

    /** mapper 必须只消费调用方给出的精确版本，不能按 tableId 回查并偷换成最新 root。 */
    @Test
    void mapsExactCallerProvidedTableVersionAndBinding() {
        DictionaryStorageMetadataMapper mapper = new DictionaryStorageMetadataMapper();
        TableDefinition version2 = table(DictionaryVersion.of(2), TableState.ACTIVE, 64);
        TableDefinition version3 = table(DictionaryVersion.of(3), TableState.ACTIVE, 96);

        MappedTableStorage mapped2 = mapper.map(version2);
        MappedTableStorage mapped3 = mapper.map(version3);

        assertSame(version2, mapped2.table());
        assertSame(version3, mapped3.table());
        assertEquals(2, mapped2.storageTable().schemaVersion());
        assertEquals(3, mapped3.storageTable().schemaVersion());
        assertEquals(PageId.of(SpaceId.of(1024), PageNo.of(64)), mapped2.clusteredIndex().rootPageId());
        assertEquals(PageId.of(SpaceId.of(1024), PageNo.of(96)), mapped3.clusteredIndex().rootPageId());
        assertTrue(mapped2.lobSegment().isEmpty(), "旧 binding 必须显式暴露 externalization unavailable");
        assertEquals(0, mapped2.clusteredIndex().keyDef().parts().getFirst().columnId().value());
        assertTrue(mapped2.clusteredIndex().clustered());
    }

    /** 未完成物理 CREATE、已 DROPPED 或请求不存在 index 都必须 fail-closed。 */
    @Test
    void rejectsDefinitionsThatCannotProduceAuthoritativeStorageMetadata() {
        DictionaryStorageMetadataMapper mapper = new DictionaryStorageMetadataMapper();
        TableDefinition withoutBinding = logicalTable(TableState.ACTIVE);
        TableDefinition dropped = table(DictionaryVersion.of(2), TableState.DROPPED, 64);

        assertThrows(DictionaryStorageMappingException.class, () -> mapper.map(withoutBinding));
        assertThrows(DictionaryStorageMappingException.class, () -> mapper.map(dropped));
        MappedTableStorage mapped = mapper.map(table(DictionaryVersion.of(2), TableState.ACTIVE, 64));
        assertThrows(DictionaryStorageMappingException.class, () -> mapped.index(999));
    }

    /** secondary descriptor 必须使用紧凑 entry schema和完整主键后缀，logical unique 不得覆盖物理唯一语义。 */
    @Test
    void mapsSecondaryIndexToCompactPhysicalMetadata() {
        DictionaryStorageMetadataMapper mapper = new DictionaryStorageMetadataMapper();

        MappedTableStorage mapped = mapper.map(tableWithSecondary());

        var tableIndexes = mapped.tableIndexes();
        assertEquals(1, tableIndexes.secondaryIndexes().size());
        var secondary = tableIndexes.secondaryIndexes().getFirst();
        assertEquals(4, secondary.index().indexId());
        assertTrue(secondary.index().physicalUnique(), "完整 secondary key 必须按二级 key + PK 做物理唯一");
        assertTrue(secondary.logicalUnique(), "DD logical unique 必须独立保留");
        assertEquals(List.of(1, 0), secondary.layout().sourceOrdinals());
        assertEquals(2, secondary.index().schema().columnCount());
        assertEquals(2, secondary.index().keyDef().parts().size());
        assertEquals(1, secondary.layout().logicalKeyPartCount());
    }

    /**
     * CREATE INDEX 只改变 DD aggregate 版本；mapper 必须继续使用 binding 中的物理行格式版本，
     * 否则既有聚簇记录会因 schema version 不匹配而全部不可读。
     */
    @Test
    void mapsMetadataOnlyIndexVersionWithPreservedPhysicalRowFormat() {
        TableDefinition version5 = tableWithSecondary();
        TableStorageBinding before = version5.storageBinding().orElseThrow();
        TableStorageBinding physicalVersion2 = new TableStorageBinding(
                before.tableId(), before.spaceId(), before.path(), 2, before.indexes(), before.lobSegment());
        TableDefinition metadataVersion5 = new TableDefinition(
                version5.id(), version5.schemaId(), version5.name(), DictionaryVersion.of(5),
                version5.state(), version5.columns(), version5.indexes(), Optional.of(physicalVersion2));

        MappedTableStorage mapped = new DictionaryStorageMetadataMapper().map(metadataVersion5);

        assertEquals(5, mapped.table().version().value());
        assertEquals(2, mapped.storageTable().schemaVersion());
        assertEquals(2, mapped.tableIndexes().clusteredIndex().schema().schemaVersion());
        assertEquals(2, mapped.tableIndexes().secondaryIndexes().getFirst()
                .index().schema().schemaVersion());
    }

    private TableDefinition table(DictionaryVersion version, TableState state, long rootPageNo) {
        TableDefinition logical = logicalTable(state);
        SegmentRef leaf = new SegmentRef(SpaceId.of(1024), 1, SegmentId.of(11));
        SegmentRef nonLeaf = new SegmentRef(SpaceId.of(1024), 2, SegmentId.of(12));
        TableStorageBinding binding = new TableStorageBinding(2, SpaceId.of(1024),
                directory.resolve("tables/table_2_space_1024.ibd"), version.value(), List.of(new IndexStorageBinding(3,
                PageId.of(SpaceId.of(1024), PageNo.of(rootPageNo)), 0, leaf, nonLeaf)), Optional.empty());
        return new TableDefinition(logical.id(), logical.schemaId(), logical.name(), version, state,
                logical.columns(), logical.indexes(), Optional.of(binding));
    }

    private static TableDefinition logicalTable(TableState state) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(2), state, List.of(id), List.of(primary));
    }

    private TableDefinition tableWithSecondary() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        ColumnDefinition email = new ColumnDefinition(2, ObjectName.of("email"),
                new ColumnTypeDefinition(cn.zhangyis.db.dd.domain.DictionaryTypeId.VARCHAR,
                        false, false, 128, 0, 1, 2, List.of()), 1);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        IndexDefinition secondary = new IndexDefinition(IndexId.of(4), ObjectName.of("uk_email"), true, false,
                List.of(new IndexKeyPart(2, IndexOrder.ASC, 0)));
        SegmentRef primaryLeaf = new SegmentRef(SpaceId.of(1024), 1, SegmentId.of(11));
        SegmentRef primaryNonLeaf = new SegmentRef(SpaceId.of(1024), 2, SegmentId.of(12));
        SegmentRef secondaryLeaf = new SegmentRef(SpaceId.of(1024), 3, SegmentId.of(13));
        SegmentRef secondaryNonLeaf = new SegmentRef(SpaceId.of(1024), 4, SegmentId.of(14));
        TableStorageBinding binding = new TableStorageBinding(2, SpaceId.of(1024),
                directory.resolve("tables/table_2_space_1024.ibd"), 5, List.of(
                new IndexStorageBinding(3, PageId.of(SpaceId.of(1024), PageNo.of(64)), 0,
                        primaryLeaf, primaryNonLeaf),
                new IndexStorageBinding(4, PageId.of(SpaceId.of(1024), PageNo.of(65)), 0,
                        secondaryLeaf, secondaryNonLeaf)), Optional.empty());
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("accounts"),
                DictionaryVersion.of(5), TableState.ACTIVE, List.of(id, email),
                List.of(primary, secondary), Optional.of(binding));
    }
}
