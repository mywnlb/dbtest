package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Binder 测试共享的真实 DD/catalog/MDL fixture；不以 mock 掩盖 lease 生命周期。 */
final class BinderTestFixture implements AutoCloseable {
    final FileInternalCatalogStore store;
    final MetadataLockManager locks;
    final DictionaryObjectCache cache;
    final DataDictionaryService dictionary;

    BinderTestFixture(Path directory) {
        store = FileInternalCatalogStore.openOrCreate(directory.resolve("mysql.ibd"));
        PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
        try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
            transaction.createSchema(new SchemaDefinition(SchemaId.of(1), ObjectName.of("app"), 1, 1,
                    DictionaryVersion.of(2)));
            transaction.createTable(orders(directory));
            transaction.createTable(prefixPrimary(directory));
            transaction.createTable(lobPrimary(directory));
            transaction.createTable(unbound());
            transaction.commit();
        }
        locks = new MetadataLockManager(4, 64);
        cache = new DictionaryObjectCache(16);
        dictionary = new DataDictionaryService(repository, cache, locks);
    }

    private static TableDefinition orders(Path directory) {
        List<ColumnDefinition> columns = List.of(
                new ColumnDefinition(1, ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false), 0),
                new ColumnDefinition(2, ObjectName.of("tenant"), ColumnTypeDefinition.integer(false, false), 1),
                new ColumnDefinition(3, ObjectName.of("note"), new ColumnTypeDefinition(DictionaryTypeId.VARCHAR,
                        false, true, 128, 0, 1, 1, List.of()), 2));
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0), new IndexKeyPart(2, IndexOrder.ASC, 0)));
        return table(2, 3, "orders", columns, primary, directory, Optional.of(segment(5, 3, 3)));
    }

    private static TableDefinition prefixPrimary(Path directory) {
        ColumnDefinition code = new ColumnDefinition(11, ObjectName.of("code"),
                new ColumnTypeDefinition(DictionaryTypeId.VARCHAR, false, false, 64, 0, 1, 1, List.of()), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(13), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(11, IndexOrder.ASC, 4)));
        return table(12, 13, "prefix_key", List.of(code), primary, directory, Optional.empty());
    }

    private static TableDefinition lobPrimary(Path directory) {
        ColumnDefinition body = new ColumnDefinition(21, ObjectName.of("body"),
                new ColumnTypeDefinition(DictionaryTypeId.TEXT, false, false, 65535, 0, 1, 1, List.of()), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(23), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(21, IndexOrder.ASC, 0)));
        return table(22, 23, "lob_key", List.of(body), primary, directory, Optional.of(segment(25, 3, 3)));
    }

    private static TableDefinition unbound() {
        ColumnDefinition id = new ColumnDefinition(31, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(33), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(31, IndexOrder.ASC, 0)));
        return new TableDefinition(TableId.of(32), SchemaId.of(1), ObjectName.of("unbound"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id), List.of(primary));
    }

    private static TableDefinition table(long tableId, long indexId, String name, List<ColumnDefinition> columns,
                                         IndexDefinition primary, Path directory, Optional<SegmentRef> lob) {
        int spaceValue = Math.toIntExact(tableId + 3);
        SpaceId spaceId = SpaceId.of(spaceValue);
        IndexStorageBinding index = new IndexStorageBinding(indexId, PageId.of(spaceId, PageNo.of(10)), 0,
                segment(spaceValue, 1, 1), segment(spaceValue, 2, 2));
        TableStorageBinding binding = new TableStorageBinding(tableId, spaceId,
                directory.resolve(name + ".ibd"), List.of(index), lob);
        return new TableDefinition(TableId.of(tableId), SchemaId.of(1), ObjectName.of(name),
                DictionaryVersion.of(2), TableState.ACTIVE, columns, List.of(primary), Optional.of(binding));
    }

    private static SegmentRef segment(int spaceId, int slot, long segmentId) {
        return new SegmentRef(SpaceId.of(spaceId), slot, SegmentId.of(segmentId));
    }

    @Override
    public void close() {
        store.close();
    }
}
