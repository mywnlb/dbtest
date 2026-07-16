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
            transaction.commit();
        }
        service = new DataDictionaryService(repository, new DictionaryObjectCache(8),
                new MetadataLockManager(4, 64));
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
