package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** 物理 DDL DTO 不变量测试：损坏 DD 不得把重复 index 或跨 space segment 传入 B+Tree。 */
class StorageDdlDomainTest {

    /**
     * metadata-only CREATE INDEX 可以推进字典版本，但必须保留聚簇记录编码时使用的物理行格式版本。
     */
    @Test
    void keepsPhysicalRowFormatVersionIndependentFromDictionaryVersion() {
        SpaceId tableSpace = SpaceId.of(1024);
        IndexStorageBinding index = new IndexStorageBinding(3,
                PageId.of(tableSpace, PageNo.of(8)), 0,
                new SegmentRef(tableSpace, 1, SegmentId.of(11)),
                new SegmentRef(tableSpace, 2, SegmentId.of(12)));

        TableStorageBinding binding = new TableStorageBinding(
                2, tableSpace, Path.of("table.ibd"), 7, List.of(index), Optional.empty());

        assertEquals(7, binding.rowFormatVersion());
        assertThrows(DatabaseValidationException.class, () -> new TableStorageBinding(
                2, tableSpace, Path.of("table.ibd"), 0, List.of(index), Optional.empty()));
    }

    /** 稳定 indexId 不可因名称不同而在同一建表请求内重复。 */
    @Test
    void rejectsDuplicateStorageIndexIdentity() {
        StorageIndexDefinition primary = index(3, "PRIMARY", true);
        StorageIndexDefinition duplicate = index(3, "idx_id", false);

        assertThrows(DatabaseValidationException.class, () -> new StorageTableDefinition(
                2, SpaceId.of(1024), Path.of("table.ibd"), 2, PageNo.of(64), columns(),
                List.of(primary, duplicate)));
    }

    /** clustered root 必须 unique，且同一 table 的物理 index name 不能重复。 */
    @Test
    void rejectsNonUniqueClusteredIndexAndDuplicateIndexName() {
        assertThrows(DatabaseValidationException.class, () -> new StorageIndexDefinition(
                3, "PRIMARY", false, true,
                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))));
        StorageIndexDefinition primary = index(3, "PRIMARY", true);
        StorageIndexDefinition duplicateName = index(4, "PRIMARY", false);
        assertThrows(DatabaseValidationException.class, () -> new StorageTableDefinition(
                2, SpaceId.of(1024), Path.of("table.ibd"), 2, PageNo.of(64), columns(),
                List.of(primary, duplicateName)));
    }

    /** root、leaf segment 和 non-leaf segment 必须全部属于 table binding 的同一 space。 */
    @Test
    void rejectsIndexBindingWhoseSegmentBelongsToAnotherTablespace() {
        SpaceId tableSpace = SpaceId.of(1024);
        IndexStorageBinding index = new IndexStorageBinding(3,
                PageId.of(tableSpace, PageNo.of(8)), 0,
                new SegmentRef(SpaceId.of(2048), 1, SegmentId.of(11)),
                new SegmentRef(tableSpace, 2, SegmentId.of(12)));

        assertThrows(DatabaseValidationException.class, () -> new TableStorageBinding(
                2, tableSpace, Path.of("table.ibd"), List.of(index), Optional.empty()));
    }

    /** LOB segment 是 table binding 的独占物理资源，不得跨 space 或复用任一 index segment identity。 */
    @Test
    void rejectsInvalidOrAliasedTableLobSegment() {
        SpaceId tableSpace = SpaceId.of(1024);
        SegmentRef leaf = new SegmentRef(tableSpace, 1, SegmentId.of(11));
        SegmentRef nonLeaf = new SegmentRef(tableSpace, 2, SegmentId.of(12));
        IndexStorageBinding index = new IndexStorageBinding(3,
                PageId.of(tableSpace, PageNo.of(8)), 0, leaf, nonLeaf);

        assertThrows(DatabaseValidationException.class, () -> new TableStorageBinding(
                2, tableSpace, Path.of("table.ibd"), List.of(index), Optional.of(leaf)));
        assertThrows(DatabaseValidationException.class, () -> new TableStorageBinding(
                2, tableSpace, Path.of("table.ibd"), List.of(index), Optional.of(
                        new SegmentRef(SpaceId.of(2048), 3, SegmentId.of(13)))));
    }

    private static List<StorageColumnDefinition> columns() {
        return List.of(new StorageColumnDefinition(1, "id", 0,
                StorageColumnType.bigint(false, false)));
    }

    private static StorageIndexDefinition index(long id, String name, boolean clustered) {
        return new StorageIndexDefinition(id, name, clustered, clustered,
                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)));
    }
}
