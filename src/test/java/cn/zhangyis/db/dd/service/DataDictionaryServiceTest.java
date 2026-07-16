package cn.zhangyis.db.dd.service;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.mdl.MdlDuration;
import cn.zhangyis.db.dd.mdl.MdlKey;
import cn.zhangyis.db.dd.mdl.MdlMode;
import cn.zhangyis.db.dd.mdl.MdlRequest;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 安全表访问租约 TDD：一次 lookup 必须同时保护名称身份与不可变 DD 版本，且任一失败路径都释放已取得的资源。
 */
class DataDictionaryServiceTest {

    @TempDir
    Path directory;

    /** 执行者持有 lease 时 DROP 的 MDL X 不能越过；close 后 X 才能授予。 */
    @Test
    void tableLeasePinsVersionAndHoldsSharedMetadataLock() throws Exception {
        try (Fixture fixture = fixture()) {
            TableMetadataLease lease = fixture.service.openTable(MdlOwnerId.of(10),
                    QualifiedTableName.of("APP", "ORDERS"), Duration.ofSeconds(1));
            assertEquals(TableId.of(2), lease.table().id());
            assertEquals(DictionaryVersion.of(2), lease.version());

            CountDownLatch started = new CountDownLatch(1);
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var exclusive = executor.submit(() -> {
                    started.countDown();
                    try (var ticket = fixture.locks.acquire(new MdlRequest(MdlOwnerId.of(11),
                            MdlKey.table(QualifiedTableName.of("app", "orders").canonicalKey()),
                            MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), Duration.ofSeconds(2))) {
                        return true;
                    }
                });
                assertTrue(started.await(1, TimeUnit.SECONDS));
                Thread.sleep(50);
                assertEquals(1, fixture.locks.snapshot().waiting().size());

                lease.close();
                assertTrue(exclusive.get(1, TimeUnit.SECONDS));
            }
            assertTrue(fixture.cache.awaitUnpinned(TableId.of(2), Duration.ofSeconds(1)));
        }
    }

    /** 名称不存在时 service 必须先释放已取得的 MDL，后续同 key 的 X 请求不能被幽灵持有者阻塞。 */
    @Test
    void missingTableDoesNotLeakMetadataLockOrCachePin() {
        try (Fixture fixture = fixture()) {
            QualifiedTableName missing = QualifiedTableName.of("app", "missing");
            assertThrows(DictionaryObjectNotFoundException.class, () -> fixture.service.openTable(
                    MdlOwnerId.of(20), missing, Duration.ofSeconds(1)));

            try (var ticket = fixture.locks.acquire(new MdlRequest(MdlOwnerId.of(21),
                    MdlKey.table(missing.canonicalKey()), MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION),
                    Duration.ofMillis(200))) {
                assertEquals(MdlMode.EXCLUSIVE, ticket.mode());
            }
            assertTrue(fixture.locks.snapshot().granted().isEmpty());
        }
    }

    private Fixture fixture() {
        FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(directory.resolve("mysql.ibd"));
        PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
        try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
            transaction.createSchema(new SchemaDefinition(SchemaId.of(1), ObjectName.of("app"), 1, 1,
                    DictionaryVersion.of(2)));
            transaction.createTable(table());
            transaction.commit();
        }
        MetadataLockManager locks = new MetadataLockManager(4, 64);
        DictionaryObjectCache cache = new DictionaryObjectCache(8);
        return new Fixture(store, locks, cache,
                new DataDictionaryService(repository, cache, locks));
    }

    private static TableDefinition table() {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(2), TableState.ACTIVE, List.of(id), List.of(primary));
    }

    private record Fixture(FileInternalCatalogStore store, MetadataLockManager locks,
                           DictionaryObjectCache cache, DataDictionaryService service) implements AutoCloseable {
        @Override
        public void close() {
            store.close();
        }
    }
}
