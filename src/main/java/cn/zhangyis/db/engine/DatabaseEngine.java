package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.engine.adapter.DictionaryIndexMetadataResolver;
import cn.zhangyis.db.dd.ddl.DictionaryDdlService;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.recovery.DictionaryDdlRecoveryService;
import cn.zhangyis.db.dd.recovery.DictionaryTablespaceDiscovery;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionaryIdRequest;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.EngineTablespaceConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据库实例组合根：先从持久 DD discovery 用户表空间，再启动 StorageEngine crash recovery，最后续作 DDL recovery
 * 并发布 dictionary/DDL facade。上层不再需要手工拼接 {@code recoveryTablespaces} 或持有 catalog 实现。
 *
 * <p>教学简化：当前 {@code mysql.ibd} 使用 page-aligned append-only catalog adapter，而非真正的内部 DD B+Tree；
 * Repository/Facade 边界已经稳定，后续可替换后端而不改变 SQL/session 或物理 DDL 协议。
 */
@Slf4j
public final class DatabaseEngine implements AutoCloseable {

    private static final SpaceId DICTIONARY_SPACE_ID = SpaceId.of(1);
    private static final int FIRST_USER_SPACE_ID = 1024;
    private static final int DICTIONARY_CACHE_CAPACITY = 256;

    /** 只保护组合根 open/close 状态，不参与任何页、MDL 或事务操作。 */
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final EngineConfig baseConfig;

    private volatile DatabaseEngineState state = DatabaseEngineState.NEW;
    private FileInternalCatalogStore catalogStore;
    private DictionaryControlStore controlStore;
    private PersistentDictionaryRepository repository;
    private DictionaryObjectCache cache;
    private MetadataLockManager metadataLocks;
    private StorageEngine storage;
    private DataDictionaryService dictionary;
    private DictionaryDdlService ddl;

    public DatabaseEngine(EngineConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("database engine config must not be null");
        }
        if (config.undoSpaceId().equals(DICTIONARY_SPACE_ID)) {
            throw new DatabaseValidationException("undo space id conflicts with dictionary space id");
        }
        this.baseConfig = config;
    }

    /**
     * 启动顺序：打开 DD catalog/control → 从 committed binding discovery → StorageEngine recovery → 构造 facade
     * → 续作 DROP_PENDING/清 orphan → 发布 OPEN。任一失败都关闭已创建资源并保持 FAILED。
     */
    public void open() {
        lifecycleLock.lock();
        try {
            if (state != DatabaseEngineState.NEW) {
                throw new DatabaseRuntimeException("database engine cannot open from state: " + state);
            }
            state = DatabaseEngineState.OPENING;
            try {
                ensureBaseDirectory();
                Path catalogPath = baseConfig.baseDir().resolve("mysql.ibd");
                Path controlPath = baseConfig.baseDir().resolve("mysql.dd.ctrl");
                Path tablesDirectory = baseConfig.baseDir().resolve("tables");
                catalogStore = FileInternalCatalogStore.openOrCreate(catalogPath);
                controlStore = DictionaryControlStore.openOrCreate(controlPath, DICTIONARY_SPACE_ID,
                        FIRST_USER_SPACE_ID);
                repository = new PersistentDictionaryRepository(catalogStore);
                reconcileControlHighWater();
                DictionaryTablespaceDiscovery discovery =
                        new DictionaryTablespaceDiscovery(repository, tablesDirectory);
                List<EngineTablespaceConfig> tablespaces = discovery.discover().stream()
                        .map(binding -> new EngineTablespaceConfig(binding.spaceId(), binding.path()))
                        .toList();
                storage = new StorageEngine(baseConfig.withRecoveryTablespaces(tablespaces));
                storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
                storage.open();

                cache = new DictionaryObjectCache(DICTIONARY_CACHE_CAPACITY);
                metadataLocks = new MetadataLockManager();
                dictionary = new DataDictionaryService(repository, cache, metadataLocks);
                ddl = new DictionaryDdlService(controlStore, repository, cache, metadataLocks,
                        storage.tableDdlStorageService(), tablesDirectory);
                new DictionaryDdlRecoveryService(controlStore, repository, cache,
                        storage.tableDdlStorageService(), tablesDirectory).recover(baseConfig.flushTimeout());
                state = DatabaseEngineState.OPEN;
                log.info("database engine opened: tablespaces={} dictionaryVersion={}", tablespaces.size(),
                        repository.snapshot().publishedVersion().value());
            } catch (RuntimeException failure) {
                state = DatabaseEngineState.FAILED;
                closeResources(failure);
                throw failure;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    /** 返回强制 MDL+pin 租约语义的 DD 读取 facade。 */
    public DataDictionaryService dictionary() {
        requireOpen();
        return dictionary;
    }

    /** 返回逻辑 DD 与物理 tablespace 的统一 DDL 协调器。 */
    public DictionaryDdlService ddl() {
        requireOpen();
        return ddl;
    }

    /** 返回已完成 discovery/recovery 的底层存储组合根。 */
    public StorageEngine storage() {
        requireOpen();
        return storage;
    }

    /** 返回当前组合根生命周期状态，不触发任何 IO。 */
    public DatabaseEngineState state() {
        return state;
    }

    /** 先关闭会 flush 用户表的 StorageEngine，再关闭 DD sidecar channel；所有失败聚合保留。 */
    @Override
    public void close() {
        lifecycleLock.lock();
        try {
            if (state == DatabaseEngineState.CLOSED) {
                return;
            }
            RuntimeException aggregate = closeResources(null);
            state = DatabaseEngineState.CLOSED;
            if (aggregate != null) {
                throw aggregate;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private RuntimeException closeResources(RuntimeException primary) {
        RuntimeException failure = primary;
        if (storage != null) {
            try {
                storage.close();
            } catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
        }
        storage = null;
        failure = closeOne(controlStore, failure);
        controlStore = null;
        failure = closeOne(catalogStore, failure);
        catalogStore = null;
        return failure;
    }

    private static RuntimeException closeOne(AutoCloseable resource, RuntimeException failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
            return failure;
        } catch (Exception closeFailure) {
            RuntimeException wrapped = closeFailure instanceof RuntimeException runtime
                    ? runtime : new DatabaseRuntimeException("close database resource failed", closeFailure);
            return appendFailure(failure, wrapped);
        }
    }

    private static RuntimeException appendFailure(RuntimeException failure, RuntimeException additional) {
        if (failure == null) {
            return additional;
        }
        failure.addSuppressed(additional);
        return failure;
    }

    private void ensureBaseDirectory() {
        try {
            Files.createDirectories(baseConfig.baseDir());
        } catch (IOException e) {
            throw new DatabaseRuntimeException("create database base directory failed: " + baseConfig.baseDir(), e);
        }
    }

    /**
     * 双槽 control 若因 latest 槽损坏回退到上一代，committed catalog 可能已经引用更高身份。启动时把各 next-counter
     * 单调推进到 catalog 最大值之后，随后才允许 recovery/DDL reserve，避免 ID/version 重用。DDL id 当前无持久行，
     * 只保持 control 自身高水位，不从 catalog 推断。
     */
    private void reconcileControlHighWater() {
        DictionaryControlSnapshot control = controlStore.snapshot();
        var snapshot = repository.snapshot();
        long nextSchema = nextAfter(snapshot.schemas().keySet().stream().mapToLong(id -> id.value())
                .max().orElse(1L), "schema");
        long nextTable = nextAfter(snapshot.tables().keySet().stream().mapToLong(id -> id.value())
                .max().orElse(0L), "table");
        long nextIndex = nextAfter(snapshot.indexes().keySet().stream().mapToLong(id -> id.value())
                .max().orElse(0L), "index");
        long nextSpace = nextAfter(snapshot.tables().values().stream()
                .flatMap(table -> table.storageBinding().stream())
                .mapToLong(binding -> binding.spaceId().value()).max().orElse(FIRST_USER_SPACE_ID - 1L), "space");
        long nextVersion = nextAfter(snapshot.publishedVersion().value(), "version");
        int schemaCount = advanceCount(control.nextSchemaId(), nextSchema, "schema");
        int tableCount = advanceCount(control.nextTableId(), nextTable, "table");
        int indexCount = advanceCount(control.nextIndexId(), nextIndex, "index");
        int spaceCount = advanceCount(control.nextSpaceId(), nextSpace, "space");
        int versionCount = advanceCount(control.nextDictionaryVersion(), nextVersion, "version");
        if ((long) schemaCount + tableCount + indexCount + spaceCount + versionCount > 0) {
            controlStore.reserve(new DictionaryIdRequest(schemaCount, tableCount, indexCount, spaceCount,
                    0, versionCount));
        }
    }

    private static int advanceCount(long current, long required, String kind) {
        if (current >= required) {
            return 0;
        }
        try {
            return Math.toIntExact(required - current);
        } catch (ArithmeticException overflow) {
            throw new DatabaseRuntimeException("dictionary " + kind + " control reconciliation range too large",
                    overflow);
        }
    }

    private static long nextAfter(long value, String kind) {
        try {
            return Math.addExact(value, 1L);
        } catch (ArithmeticException overflow) {
            throw new DatabaseRuntimeException("dictionary " + kind + " identity range exhausted", overflow);
        }
    }

    private void requireOpen() {
        if (state != DatabaseEngineState.OPEN) {
            throw new DatabaseRuntimeException("database engine is not OPEN: " + state);
        }
    }
}
