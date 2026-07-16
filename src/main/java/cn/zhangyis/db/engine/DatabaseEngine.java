package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.engine.adapter.DictionaryIndexMetadataResolver;
import cn.zhangyis.db.engine.adapter.DictionaryStorageMetadataMapper;
import cn.zhangyis.db.engine.adapter.DefaultSqlStorageGateway;
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
import cn.zhangyis.db.session.DefaultSqlSession;
import cn.zhangyis.db.session.SessionId;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.session.SessionRegistry;
import cn.zhangyis.db.session.SqlSession;
import cn.zhangyis.db.sql.binder.DefaultSqlBinder;
import cn.zhangyis.db.sql.binder.SqlTypeCoercion;
import cn.zhangyis.db.sql.executor.DefaultSqlExecutor;
import cn.zhangyis.db.sql.parser.DefaultSqlParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    /** 序列化可能跨 lifecycle lock 外等待 Session 的 close 尝试；使用有界 tryLock。 */
    private final ReentrantLock closeWorkLock = new ReentrantLock(true);
    private final EngineConfig baseConfig;
    /** 用户语句并发 read / shutdown write gate；不串行不同 Session。 */
    private final EngineSessionExecutionGate sessionExecutionGate;

    private volatile DatabaseEngineState state = DatabaseEngineState.NEW;
    private FileInternalCatalogStore catalogStore;
    private DictionaryControlStore controlStore;
    private PersistentDictionaryRepository repository;
    private DictionaryObjectCache cache;
    private MetadataLockManager metadataLocks;
    private StorageEngine storage;
    private DataDictionaryService dictionary;
    private DictionaryDdlService ddl;
    private DictionaryStorageMetadataMapper sqlMetadataMapper;
    private DefaultSqlParser sqlParser;
    private DefaultSqlBinder sqlBinder;
    private SessionRegistry sessions;

    public DatabaseEngine(EngineConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("database engine config must not be null");
        }
        if (config.undoSpaceId().equals(DICTIONARY_SPACE_ID)) {
            throw new DatabaseValidationException("undo space id conflicts with dictionary space id");
        }
        this.baseConfig = config;
        this.sessionExecutionGate = new EngineSessionExecutionGate(this::state, this::failClosed);
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
                // SQL/session 只能在 storage recovery 与 DDL recovery 都成功后发布，避免半恢复实例接受用户语句。
                sqlMetadataMapper = new DictionaryStorageMetadataMapper();
                sqlParser = new DefaultSqlParser();
                sqlBinder = new DefaultSqlBinder(new SqlTypeCoercion());
                sessions = new SessionRegistry();
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

    /**
     * 创建一个进程内 SQL Session。构造、初始 implicit transaction 和 registry publish 都在 lifecycle lock 内，
     * 因而 close 一旦切到 CLOSING，后续 openSession 不可能漏进 close 快照。
     */
    public SqlSession openSession(SessionOptions options) {
        if (options == null) throw new DatabaseValidationException("session options must not be null");
        lifecycleLock.lock();
        try {
            requireOpen();
            SessionId id = sessions.nextId();
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(storage, sqlMetadataMapper,
                    options.rowLockTimeout());
            DefaultSqlSession session = new DefaultSqlSession(id, options, dictionary, sqlParser, sqlBinder,
                    new DefaultSqlExecutor(gateway), gateway, sessionExecutionGate,
                    () -> sessions.deregister(id));
            sessions.register(session);
            return session;
        } finally {
            lifecycleLock.unlock();
        }
    }

    /** 返回当前组合根生命周期状态，不触发任何 IO。 */
    public DatabaseEngineState state() {
        return state;
    }

    /**
     * 关闭数据库组合根。
     * <ol>
     *     <li>有界取得 close owner 锁，拒绝两个线程同时执行资源终结。</li>
     *     <li>在 lifecycle 短锁内切 CLOSING 并冻结 Session 快照；从此 openSession 和新 execute 均被拒绝。</li>
     *     <li>在 lifecycle 锁外取得 statement gate write permit，有界等待所有已准入 execute 完整退出。</li>
     *     <li>持 write permit 并发关闭快照 Session，使活动事务先 rollback、再释放 transaction-duration MDL/pin。</li>
     *     <li>仅在语句已静默后关闭 StorageEngine 与 DD sidecar，发布 CLOSED；任一 close 异常用 suppressed 聚合。</li>
     * </ol>
     * 若第 1、3、4 步 timeout，保持 CLOSING 且绝不关闭仍可能被执行线程访问的 storage。
     */
    @Override
    public void close() {
        // 1. closeWorkLock 只串行 shutdown owner，不保护 storage/session 内部状态。
        boolean closeOwner;
        try {
            closeOwner = closeWorkLock.tryLock(baseConfig.flushTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException("database close wait interrupted", interrupted);
        }
        if (!closeOwner) throw new DatabaseRuntimeException("database close already in progress beyond timeout");
        try {
            // 2. 状态与 snapshot 同批发布，消除 openSession 漏入关闭快照的窗口。
            List<SqlSession> snapshot;
            lifecycleLock.lock();
            try {
                if (state == DatabaseEngineState.CLOSED) return;
                state = DatabaseEngineState.CLOSING;
                snapshot = sessions == null ? List.of() : sessions.snapshot();
            } finally {
                lifecycleLock.unlock();
            }

            // 3. write permit 覆盖后续 Session rollback 与 storage/DD close；timeout 时不触碰任何下游资源。
            try (var ignored = sessionExecutionGate.awaitQuiescence(baseConfig.flushTimeout())) {
                // 4. 此时不存在持 operationLock 的 execute，Session.close 不会与 gate 形成反向锁序。
                SessionCloseResult sessionResult = closeSessions(snapshot, baseConfig.flushTimeout());
                if (!sessionResult.quiesced()) {
                    throw sessionResult.failure();
                }
                // 5. storage 关闭异常仍继续清 DD sidecar，并在 CLOSED 发布后把聚合错误返回调用方。
                RuntimeException aggregate = closeResources(sessionResult.failure());
                lifecycleLock.lock();
                try { state = DatabaseEngineState.CLOSED; }
                finally { lifecycleLock.unlock(); }
                if (aggregate != null) throw aggregate;
            }
        } finally {
            closeWorkLock.unlock();
        }
    }

    /**
     * 用 virtual thread 并发请求 snapshot 中所有 Session close，共享单一 engine deadline。close 异常只聚合，仍等待
     * 其它 Session；超时/中断则取消等待并返回 quiesced=false，禁止下游资源关闭。
     */
    private static SessionCloseResult closeSessions(List<SqlSession> snapshot, java.time.Duration timeout) {
        if (snapshot.isEmpty()) return new SessionCloseResult(true, null);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<RuntimeException>> futures = new ArrayList<>(snapshot.size());
        for (SqlSession session : snapshot) {
            futures.add(executor.submit(() -> {
                try { session.close(); return null; }
                catch (RuntimeException failure) { return failure; }
            }));
        }
        long deadline = deadline(timeout);
        RuntimeException failure = null;
        boolean quiesced = true;
        try {
            for (Future<RuntimeException> future : futures) {
                long remaining = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
                if (remaining <= 0) throw new TimeoutException("session close deadline expired");
                RuntimeException sessionFailure = future.get(remaining, TimeUnit.NANOSECONDS);
                failure = appendFailure(failure, sessionFailure);
            }
        } catch (TimeoutException timeoutFailure) {
            quiesced = false;
            failure = appendFailure(failure,
                    new DatabaseRuntimeException("active sessions did not quiesce before engine close timeout",
                            timeoutFailure));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            quiesced = false;
            failure = appendFailure(failure,
                    new DatabaseRuntimeException("engine session close wait interrupted", interrupted));
        } catch (ExecutionException impossible) {
            failure = appendFailure(failure,
                    new DatabaseRuntimeException("session close task failed outside captured runtime error", impossible));
        } finally {
            if (!quiesced) for (Future<RuntimeException> future : futures) future.cancel(true);
            executor.shutdownNow();
        }
        return new SessionCloseResult(quiesced, failure);
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
        sqlMetadataMapper = null;
        sqlParser = null;
        sqlBinder = null;
        sessions = null;
        failure = closeOne(controlStore, failure);
        controlStore = null;
        failure = closeOne(catalogStore, failure);
        catalogStore = null;
        return failure;
    }

    /**
     * 任一 Session 观察到 DatabaseFatalException 时立即关闭新 Session/statement 准入。这里只发布状态并记录根因，
     * 不在活动 execute 线程中递归 close/flush；调用方随后显式 close，由 statement gate 先完成 quiescence。
     */
    private void failClosed(cn.zhangyis.db.common.exception.DatabaseFatalException failure) {
        lifecycleLock.lock();
        try {
            if (state == DatabaseEngineState.OPEN) {
                state = DatabaseEngineState.FAILED;
                log.error("database engine entered FAILED after a fail-stop statement error", failure);
            }
        } finally {
            lifecycleLock.unlock();
        }
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
        if (additional == null) return failure;
        if (failure == null) {
            return additional;
        }
        failure.addSuppressed(additional);
        return failure;
    }

    private static long deadline(java.time.Duration timeout) {
        long now = System.nanoTime();
        try { return Math.addExact(now, timeout.toNanos()); }
        catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
    }

    private record SessionCloseResult(boolean quiesced, RuntimeException failure) { }

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
