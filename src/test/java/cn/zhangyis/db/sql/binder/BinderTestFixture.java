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
                        false, true, 128, 0, 1, 1, List.of()), 2),
                new ColumnDefinition(4, ObjectName.of("status"), new ColumnTypeDefinition(DictionaryTypeId.VARCHAR,
                        false, false, 32, 0, 1, 1, List.of()), 3));
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0), new IndexKeyPart(2, IndexOrder.ASC, 0)));
        IndexDefinition note = new IndexDefinition(IndexId.of(4), ObjectName.of("uq_note"), true, false,
                List.of(new IndexKeyPart(3, IndexOrder.ASC, 0)));
        IndexDefinition status = new IndexDefinition(IndexId.of(5), ObjectName.of("idx_status"), false, false,
                List.of(new IndexKeyPart(4, IndexOrder.ASC, 0)));
        return table(2, "orders", columns, List.of(primary, note, status), directory,
                Optional.of(segment(5, 30, 30)));
    }

    private static TableDefinition prefixPrimary(Path directory) {
        ColumnDefinition code = new ColumnDefinition(11, ObjectName.of("code"),
                new ColumnTypeDefinition(DictionaryTypeId.VARCHAR, false, false, 64, 0, 1, 1, List.of()), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(13), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(11, IndexOrder.ASC, 4)));
        return table(12, "prefix_key", List.of(code), List.of(primary), directory, Optional.empty());
    }

    private static TableDefinition lobPrimary(Path directory) {
        ColumnDefinition body = new ColumnDefinition(21, ObjectName.of("body"),
                new ColumnTypeDefinition(DictionaryTypeId.TEXT, false, false, 65535, 0, 1, 1, List.of()), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(23), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(21, IndexOrder.ASC, 0)));
        return table(22, "lob_key", List.of(body), List.of(primary), directory, Optional.of(segment(25, 30, 30)));
    }

    private static TableDefinition unbound() {
        ColumnDefinition id = new ColumnDefinition(31, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(33), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(31, IndexOrder.ASC, 0)));
        return new TableDefinition(TableId.of(32), SchemaId.of(1), ObjectName.of("unbound"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id), List.of(primary));
    }

    private static TableDefinition table(long tableId, String name, List<ColumnDefinition> columns,
                                         List<IndexDefinition> indexes, Path directory, Optional<SegmentRef> lob) {
        int spaceValue = Math.toIntExact(tableId + 3);
        SpaceId spaceId = SpaceId.of(spaceValue);
        List<IndexStorageBinding> bindings = new java.util.ArrayList<>();
        for (int i = 0; i < indexes.size(); i++) {
            IndexDefinition index = indexes.get(i);
            bindings.add(new IndexStorageBinding(index.id().value(), PageId.of(spaceId, PageNo.of(10 + i)), 0,
                    segment(spaceValue, 10 + i * 2, 100 + i * 2L),
                    segment(spaceValue, 11 + i * 2, 101 + i * 2L)));
        }
        TableStorageBinding binding = new TableStorageBinding(tableId, spaceId,
                directory.resolve(name + ".ibd"), bindings, lob);
        return new TableDefinition(TableId.of(tableId), SchemaId.of(1), ObjectName.of(name),
                DictionaryVersion.of(2), TableState.ACTIVE, columns, indexes, Optional.of(binding));
    }

    private static SegmentRef segment(int spaceId, int slot, long segmentId) {
        return new SegmentRef(SpaceId.of(spaceId), slot, SegmentId.of(segmentId));
    }

    @Override
    public void close() {
        store.close();
    }
}
