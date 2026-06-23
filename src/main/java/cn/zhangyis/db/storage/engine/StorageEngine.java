package cn.zhangyis.db.storage.engine;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;
import cn.zhangyis.db.storage.trx.HistoryList;
import cn.zhangyis.db.storage.trx.MvccReader;
import cn.zhangyis.db.storage.trx.RollbackSegmentSlotManager;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionSystem;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * 存储引擎组合根（engine bootstrap E1，设计 §3/§13）。把此前仅在测试中构造的存储组件接线成一个生产实例：
 * 共享单一 {@link TablespaceAccessController}，redo 用 durable {@link RedoLogManager}，使 **WAL 顺序在生产
 * flush/checkpoint 路径与 commit 之间成立**（redo durable 必先于 data page 写数据文件）。
 *
 * <p><b>生命周期</b>：{@link #open()}（fresh 建 redo/系统 undo 表空间；existing 开文件 + 安装 redo 边界使续写 LSN
 * 连续，不 replay）→ {@link #checkpoint()}/{@link #close()} 经 {@link FlushService#flushThrough} 按 WAL 顺序持久
 *（先 redo.flush 再刷脏页再持久 checkpoint），close 末关闭 AutoCloseable 句柄。
 *
 * <p><b>E1 限制</b>：不跑崩溃恢复（仅 clean-shutdown 持久，E2）；无后台线程（前台 flush/checkpoint，E3）；不接
 * {@code PurgeCoordinator}（需 per-index，E4）；用 {@link NoDoublewriteStrategy}。**假设无脏页淘汰**——
 * {@code LruBufferPool} 淘汰写回不走 WAL gate（已知缺口），故 buffer 容量须 &gt; 工作集。
 *
 * <p>访问器暴露已接线的事务/disk/btree/undo/mvcc/rollback 服务，供测试与未来 DML facade（E4）驱动；本类不含 DML 逻辑。
 */
public final class StorageEngine {

    private final EngineConfig config;
    private EngineState state = EngineState.NEW;

    // AutoCloseable 句柄（close 时关闭）
    private PageStore store;
    private RedoLogFileRepository redoRepo;
    private RedoCheckpointStore checkpointStore;
    private BufferPool pool;

    // 接线的服务
    private RedoLogManager redo;
    private MiniTransactionManager miniTransactionManager;
    private DiskSpaceManager diskSpaceManager;
    private TransactionManager transactionManager;
    private UndoLogManager undoLogManager;
    private SplitCapableBTreeIndexService btreeService;
    private IndexPageAccess indexPageAccess;
    private MvccReader mvccReader;
    private RollbackService rollbackService;
    private FlushService flushService;

    public StorageEngine(EngineConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("engine config must not be null");
        }
        this.config = config;
    }

    /**
     * 打开引擎。fresh（baseDir 无 redo.log）建 redo 文件 + 系统 undo 表空间；existing 开 redo/control + 安装 redo 边界
     * （读 checkpoint label + {@link RedoRecoveryReader#recoveredToLsn()} → {@link RedoLogManager#restoreRecoveredBoundary}，
     * 不 replay/不 repair）+ 打开系统 undo 表空间。接线全部组件并发布 OPEN。
     */
    public void open() {
        if (state != EngineState.NEW) {
            throw new EngineStateException("open requires NEW state: " + state);
        }
        try {
            Files.createDirectories(config.baseDir());
        } catch (IOException e) {
            throw new DatabaseRuntimeException("create engine baseDir failed: " + config.baseDir(), e);
        }
        boolean fresh = !Files.exists(config.redoFile());

        this.redoRepo = RedoLogFileRepository.open(config.redoFile());
        this.redo = RedoLogManager.durable(redoRepo);
        this.checkpointStore = RedoCheckpointStore.open(config.redoControlFile());
        TablespaceAccessController controller = new TablespaceAccessController();
        this.store = new FileChannelPageStore();
        this.pool = new LruBufferPool(store, config.pageSize(), config.bufferPoolCapacityFrames());
        this.miniTransactionManager = new MiniTransactionManager(controller, redo);
        this.diskSpaceManager = new DiskSpaceManager(pool, store, config.pageSize(), controller);

        TypeCodecRegistry registry = new TypeCodecRegistry();
        TransactionSystem txnSystem = new TransactionSystem();
        this.transactionManager = new TransactionManager(txnSystem);
        RollbackSegmentSlotManager slots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), config.slotCapacity());
        HistoryList history = new HistoryList();
        DiskSpaceUndoAllocator undoAllocator = new DiskSpaceUndoAllocator(diskSpaceManager);
        UndoLogSegmentAccess undoAccess = new UndoLogSegmentAccess(pool, config.pageSize(), undoAllocator, registry);
        this.undoLogManager = new UndoLogManager(undoAccess, slots, config.undoSpaceId(), history);
        this.indexPageAccess = new IndexPageAccess(pool, config.pageSize());
        this.btreeService = new SplitCapableBTreeIndexService(indexPageAccess, diskSpaceManager, registry);
        this.mvccReader = new MvccReader(miniTransactionManager, btreeService, undoAccess,
                config.undoSpaceId(), config.maxVersionHops());
        this.rollbackService = new RollbackService(btreeService, undoAccess, slots, transactionManager,
                miniTransactionManager);

        FlushCoordinator flushCoordinator = new FlushCoordinator(pool, store, redo, config.pageSize(),
                new NoDoublewriteStrategy(), config.flushTimeout(), controller);
        CheckpointCoordinator checkpointCoordinator = new CheckpointCoordinator(pool, redo, checkpointStore);
        this.flushService = new FlushService(pool, flushCoordinator, checkpointCoordinator, redo,
                RedoCapacityPolicy.fixed(config.redoCapacityBytes()),
                AdaptiveFlushPolicy.fixed(1, config.bufferPoolCapacityFrames()));

        if (fresh) {
            // 建系统 undo 表空间（page0/inode 经 MTR 写入，redo 累积，close 时随 flushThrough durable）
            MiniTransaction boot = miniTransactionManager.begin();
            diskSpaceManager.createTablespace(boot, config.undoSpaceId(), config.undoFile(),
                    config.undoSpaceInitialPages(), TablespaceType.UNDO);
            miniTransactionManager.commit(boot);
        } else {
            // 安装 redo 边界：从 durable redo 尾部续写，避免重开后新 append 从 0 重叠（评审 #5）。不 replay。
            RedoCheckpointLabel label = checkpointStore.readLatest();
            RedoRecoveryReader reader = new RedoRecoveryReader(redoRepo, label.checkpointLsn());
            reader.readBatches();
            redo.restoreRecoveredBoundary(reader.recoveredToLsn());
            diskSpaceManager.openTablespace(config.undoSpaceId(), config.undoFile());
        }
        this.state = EngineState.OPEN;
    }

    /**
     * 显式 checkpoint：按 WAL 顺序把 redo 刷盘、刷出全部 dirty page（oldest≤currentLsn）、持久化 checkpoint label。
     */
    public void checkpoint() {
        requireOpen();
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
    }

    /**
     * 关闭引擎。先 {@link FlushService#flushThrough}（WAL 顺序持久 + 清空 dirty），再依次关闭 pool（此时 dirty 已空，
     * legacy flushAll 为 no-op，不绕 WAL gate）、store、redoRepo、checkpointStore。幂等：CLOSED 再 close 为 no-op。
     */
    public void close() {
        if (state == EngineState.CLOSED) {
            return;
        }
        if (state != EngineState.OPEN) {
            throw new EngineStateException("close requires OPEN state: " + state);
        }
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
        List<RuntimeException> errors = new ArrayList<>();
        closeQuietly(pool, errors);
        closeQuietly(store, errors);
        closeQuietly(redoRepo, errors);
        closeQuietly(checkpointStore, errors);
        state = EngineState.CLOSED;
        if (!errors.isEmpty()) {
            DatabaseRuntimeException aggregate =
                    new DatabaseRuntimeException("engine close failed to release " + errors.size() + " handle(s)",
                            errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    public MiniTransactionManager miniTransactionManager() {
        requireOpen();
        return miniTransactionManager;
    }

    public TransactionManager transactionManager() {
        requireOpen();
        return transactionManager;
    }

    public DiskSpaceManager diskSpaceManager() {
        requireOpen();
        return diskSpaceManager;
    }

    public SplitCapableBTreeIndexService btreeService() {
        requireOpen();
        return btreeService;
    }

    /** 索引页格式化入口（建聚簇/二级索引 root 页用）。E4 DML/DDL 接线前供测试与上层格式化索引页。 */
    public IndexPageAccess indexPageAccess() {
        requireOpen();
        return indexPageAccess;
    }

    public UndoLogManager undoLogManager() {
        requireOpen();
        return undoLogManager;
    }

    public MvccReader mvccReader() {
        requireOpen();
        return mvccReader;
    }

    public RollbackService rollbackService() {
        requireOpen();
        return rollbackService;
    }

    /** 当前生命周期状态。 */
    public EngineState state() {
        return state;
    }

    private void requireOpen() {
        if (state != EngineState.OPEN) {
            throw new EngineStateException("engine not OPEN: " + state);
        }
    }

    private static void closeQuietly(AutoCloseable handle, List<RuntimeException> errors) {
        if (handle == null) {
            return;
        }
        try {
            handle.close();
        } catch (RuntimeException e) {
            errors.add(e);
        } catch (Exception e) {
            errors.add(new DatabaseRuntimeException("close handle failed", e));
        }
    }
}
