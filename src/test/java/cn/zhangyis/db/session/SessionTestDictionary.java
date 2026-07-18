package cn.zhangyis.db.session;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.domain.*;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Session 测试使用的真实 DD/MDL fixture，保证 binder lease cleanup 可观测。 */
final class SessionTestDictionary implements AutoCloseable {
    final FileInternalCatalogStore store;
    final DataDictionaryService service;

    SessionTestDictionary(Path directory) {
        store = FileInternalCatalogStore.openOrCreate(directory.resolve("mysql.ibd"));
        PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
        try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
            transaction.createSchema(new SchemaDefinition(SchemaId.of(1), ObjectName.of("app"), 1, 1,
                    DictionaryVersion.of(2)));
            transaction.createTable(table(directory));
            transaction.createTable(rangeTable(directory));
            transaction.commit();
        }
        service = new DataDictionaryService(repository, new DictionaryObjectCache(8),
                new MetadataLockManager(4, 64));
    }

    /** Session locking-range 测试表；ordinary secondary equality 会绑定为多行 physical prefix range。 */
    private static TableDefinition rangeTable(Path directory) {
        ColumnDefinition id = new ColumnDefinition(11, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        ColumnDefinition category = new ColumnDefinition(12, ObjectName.of("category"),
                new ColumnTypeDefinition(DictionaryTypeId.VARCHAR, false, false,
                        32, 0, 1, 1, List.of()), 1);
        IndexDefinition primary = new IndexDefinition(IndexId.of(13), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(11, IndexOrder.ASC, 0)));
        IndexDefinition categoryIndex = new IndexDefinition(IndexId.of(14), ObjectName.of("idx_category"),
                false, false, List.of(new IndexKeyPart(12, IndexOrder.ASC, 0)));
        SpaceId space = SpaceId.of(15);
        TableStorageBinding binding = new TableStorageBinding(12, space,
                directory.resolve("range_orders.ibd"), List.of(
                new IndexStorageBinding(13, PageId.of(space, PageNo.of(10)), 0,
                        new SegmentRef(space, 1, SegmentId.of(11)), new SegmentRef(space, 2, SegmentId.of(12))),
                new IndexStorageBinding(14, PageId.of(space, PageNo.of(11)), 0,
                        new SegmentRef(space, 3, SegmentId.of(13)), new SegmentRef(space, 4, SegmentId.of(14)))),
                Optional.empty());
        return new TableDefinition(TableId.of(12), SchemaId.of(1), ObjectName.of("range_orders"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id, category),
                List.of(primary, categoryIndex), Optional.of(binding));
    }

    private static TableDefinition table(Path directory) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SpaceId space = SpaceId.of(5);
        TableStorageBinding binding = new TableStorageBinding(2, space, directory.resolve("orders.ibd"), List.of(
                new IndexStorageBinding(3, PageId.of(space, PageNo.of(10)), 0,
                        new SegmentRef(space, 1, SegmentId.of(1)), new SegmentRef(space, 2, SegmentId.of(2)))),
                Optional.empty());
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id), List.of(primary), Optional.of(binding));
    }

    @Override public void close() { store.close(); }
}
