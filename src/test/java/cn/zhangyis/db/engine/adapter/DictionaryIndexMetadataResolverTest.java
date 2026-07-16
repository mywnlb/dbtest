package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** undo tableId/indexId 到 BTreeIndex 快照的 DD adapter TDD。 */
class DictionaryIndexMetadataResolverTest {

    @TempDir
    Path directory;

    /** resolver 使用持久 root/segment binding 和逻辑 schema/key，不能回退全局单索引。 */
    @Test
    void resolvesExactTableAndIndexBinding() {
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(directory.resolve("mysql.ibd"))) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
            try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
                transaction.createSchema(new SchemaDefinition(SchemaId.of(1), ObjectName.of("app"), 1, 1,
                        DictionaryVersion.of(2)));
                transaction.createTable(table());
                transaction.commit();
            }

            var index = new DictionaryIndexMetadataResolver(repository).resolve(2, 3);

            assertEquals(3, index.indexId());
            assertEquals(PageId.of(SpaceId.of(1024), PageNo.of(64)), index.rootPageId());
            assertEquals(2, index.schema().schemaVersion());
            assertTrue(index.clustered());
            assertEquals(0, index.keyDef().parts().getFirst().columnId().value());
        }
    }

    private TableDefinition table() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SegmentRef leaf = new SegmentRef(SpaceId.of(1024), 1, SegmentId.of(11));
        SegmentRef nonLeaf = new SegmentRef(SpaceId.of(1024), 2, SegmentId.of(12));
        TableStorageBinding binding = new TableStorageBinding(2, SpaceId.of(1024),
                directory.resolve("tables/table_2_space_1024.ibd"), List.of(new IndexStorageBinding(3,
                PageId.of(SpaceId.of(1024), PageNo.of(64)), 0, leaf, nonLeaf)), Optional.empty());
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id), List.of(primary), Optional.of(binding));
    }
}
