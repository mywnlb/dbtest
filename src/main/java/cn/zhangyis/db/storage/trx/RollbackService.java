package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.api.lob.LobStorage;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;

/**
 * 事务 rollback 执行器（设计 §7.6/§11.2/§14.4）。从 INSERT/UPDATE 两个局部 logical head 中反复选择较大
 * undoNo，恢复全局 DML 逆序；三类 record 分别执行删除、旧 image 恢复和取消删除标记。完整 rollback 最后原子
 * 回收全部 segments/page3 slots，savepoint rollback 则在逆操作完成后同批持久退回受影响的 heads。
 *
 * <p><b>依赖方向</b>：{@code storage.trx → storage.btree + storage.undo}（设计 §94）。本类直 import
 * {@link SplitCapableBTreeIndexService}（删除聚簇行）与 {@link UndoLogSegmentAccess}/{@link UndoRecord}（读 undo 链）；
 * btree/undo 均不反向 import trx，无环。
 *
 * <p><b>状态机两阶段</b>：{@code rollback} 在 ACTIVE 状态先预检整条逻辑链，再经
 * {@link TransactionManager#beginRollback} 进入 ROLLING_BACK；已处于 ROLLING_BACK 的失败重试直接恢复走链。
 * 走完链并经 finalizer 同批 cache/free/drop segment、转移 page3 owner、写 rollback-complete 正式恢复证据后，才调用
 * {@link TransactionManager#finishRollback}（removeActive + →ROLLED_BACK）。
 *
 * <p><b>每条 undo 短读 + 独立写 MTR</b>（§7.6 step 6）：先分别用短只读 MTR 物化当前 record 与真实前驱，
 * 再用独立 index MTR 修改聚簇页，最后用独立 marker MTR 后退 first-page logical head。marker commit 后 live
 * {@link UndoContext} 才发布相同边界；因此大事务可从逐条持久进度恢复，且不会同时持有 undo 与 index latch。
 * 单条失败时事务停在 {@code ROLLING_BACK}、slot/锁/活跃表不释放；marker 落后只会让重试幂等重复最后一条 inverse。
 *
 * <p><b>幂等</b>：B+Tree 反向命令未命中/所有权不匹配即 no-op，故 MTR 无 content undo 留下的 orphan undo
 * （已写 undo 但无对应聚簇行）由本走链幂等清理；1.4 起 statement/savepoint 边界可由 storage DML Guard
 * 显式回退。
 *
 * <p><b>索引解析</b>：legacy 调用继续显式传单聚簇索引；DatabaseEngine 注入 {@link IndexMetadataResolver} 后，
 * 每条 undo 先读取固定前缀 tableId/indexId，再解析对应 keyDef/schema/root。二级索引 undo 尚未生成；SQL/session
 * 自动 statement 生命周期与 savepoint lock scope 留后续。
 */
public final class RollbackService {

    /** 聚簇删除入口；{@code applyUndoRecord} 对 INSERT_ROW undo 调 {@code deleteClustered} 物理删除。 */
    private final SplitCapableBTreeIndexService btree;
    /** undo 物理设施；走链经它 {@code open(SHARED)} + {@code readRecord} 读回每条 undo record。 */
    private final UndoLogSegmentAccess undoAccess;
    /** 事务状态门面；提供 begin/finishRollback 两阶段，集中状态机逻辑不重复。 */
    private final TransactionManager txnMgr;
    /** 物理短事务工厂；每条 undo 至少一个短读 MTR，反向修改另用独立 index MTR。 */
    private final MiniTransactionManager mtrMgr;
    /** 完整/恢复回滚到 EMPTY 后执行 atomic cache/free/drop + page3 owner 转移的终态协作者。 */
    private final UndoSegmentFinalizer finalizer;
    /** 包内 crash-point 测试接缝；生产公开构造器固定注入 no-op，不改变正常控制流。 */
    private final RollbackProgressFaultInjector progressFaultInjector;
    /** 非 null 时每条 undo 按固定前缀身份解析真实索引；null 保留旧显式单索引调用兼容。 */
    private final IndexMetadataResolver indexResolver;
    /** 生产 full/recovery rollback 的权威 index+LOB binding 解析器；不依赖最后一次调用方索引。 */
    private final UndoTargetMetadataResolver targetResolver;
    /** INSERT ownership marker MTR 的 LOB 校验/free 入口；只有 target resolver 模式允许非空 ownership。 */
    private final LobStorage lobStorage;

    /**
     * 构造生产 rollback 执行器；逐记录 progress hook 固定为 no-op，完整终结必须使用共享 finalizer。
     *
     * @param btree      聚簇 inverse 执行入口。
     * @param undoAccess undo logical chain 读取入口。
     * @param txnMgr     live transaction 状态机。
     * @param mtrMgr     undo/index/progress 短 MTR 来源。
     * @param finalizer  EMPTY 后原子终结 undo 段的协作者。
     */
    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                           UndoSegmentFinalizer finalizer) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, RollbackProgressFaultInjector.none(),
                null, null, null);
    }

    /** DD 模式构造器：rollback/recovery 逐条解析 undo 的 tableId/indexId。 */
    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                           UndoSegmentFinalizer finalizer, IndexMetadataResolver indexResolver) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, RollbackProgressFaultInjector.none(),
                indexResolver, null, null);
    }

    /** DD 生产模式：full/recovery rollback 同时解析精确聚簇索引与权威 LOB segment。 */
    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                           UndoSegmentFinalizer finalizer, LobStorage lobStorage,
                           UndoTargetMetadataResolver targetResolver) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, RollbackProgressFaultInjector.none(),
                null, targetResolver, lobStorage);
    }

    /**
     * 包内测试构造器。只有同包 crash-point 测试可以替换 injector；生产组合根继续调用五参数公开构造器。
     */
    RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                    TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                    UndoSegmentFinalizer finalizer, RollbackProgressFaultInjector progressFaultInjector) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, progressFaultInjector,
                null, null, null);
    }

    /**
     * 复制当前生产 wiring 并只替换逐记录 crash hook。该包内接缝让故障测试复用真实 DD target 与 LOB storage，
     * 不修改原实例，也不允许上层把 injector 当成运行期配置；正常组合根始终持有 no-op 实例。
     *
     * @param injector 仅在已提交的 inverse/progress 边界后触发的故障注入器。
     * @return 与当前实例共享无状态协作者、但使用指定 crash hook 的独立回滚执行器。
     */
    RollbackService withProgressFaultInjectorForTest(RollbackProgressFaultInjector injector) {
        if (injector == null) {
            throw new DatabaseValidationException("rollback progress fault injector must not be null");
        }
        return new RollbackService(btree, undoAccess, txnMgr, mtrMgr, finalizer, injector,
                indexResolver, targetResolver, lobStorage);
    }

    private RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                    TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                    UndoSegmentFinalizer finalizer, RollbackProgressFaultInjector progressFaultInjector,
                    IndexMetadataResolver indexResolver, UndoTargetMetadataResolver targetResolver,
                    LobStorage lobStorage) {
        if (btree == null || undoAccess == null || txnMgr == null || mtrMgr == null
                || finalizer == null || progressFaultInjector == null) {
            throw new DatabaseValidationException("rollback service collaborators must not be null");
        }
        this.btree = btree;
        this.undoAccess = undoAccess;
        this.txnMgr = txnMgr;
        this.mtrMgr = mtrMgr;
        this.finalizer = finalizer;
        this.progressFaultInjector = progressFaultInjector;
        this.indexResolver = indexResolver;
        this.targetResolver = targetResolver;
        this.lobStorage = lobStorage;
    }

    /**
     * 完整回滚事务。数据流：
     * <ol>
     *   <li>从当前 logical head 预检剩余逻辑链；ACTIVE 事务通过后才 {@code beginRollback} 进入 ROLLING_BACK，
     *       ROLLING_BACK 重试保持原状态并从已发布进度继续。</li>
     *   <li>若有 {@link UndoContext}：从当前 logical head 反向走链；每条在两个短只读 MTR 中物化 current/前驱，
     *       再以独立 index MTR 执行 inverse，随后独立 marker MTR CAS 后退持久头。</li>
     *   <li>marker commit 后才发布内存 context；两次成功 commit 之间 crash 时，重启最多重复当前 inverse。</li>
     *   <li>走到 {@code prev=NULL} 后由 finalizer 在同一 MTR cache/free/drop segment、CAS 转移 page3 owner，并追加 recovery table
     *       消费的 rollback-complete 终态/高水位证据；提交成功后才发布内存 slot 释放。</li>
     *   <li>{@code finishRollback}：removeActive + ROLLING_BACK→ROLLED_BACK；若此前失败，后续调用可从
     *       ROLLING_BACK 幂等重走链。</li>
     * </ol>
     * 只读/未写事务（{@code undoContext()==null}）跳过走链，仅翻状态、不动 slot。
     *
     * @param txn            待回滚事务，必须为 ACTIVE 或可重试的 ROLLING_BACK。
     * @param clusteredIndex 该事务写入的聚簇索引（提供 keyDef/schema 解码 undo + 删除目标）。
     * @return 本次回滚应用的 undo record 条数摘要。
     */
    public RollbackSummary rollback(Transaction txn, BTreeIndex clusteredIndex) {
        if (txn == null || clusteredIndex == null) {
            throw new DatabaseValidationException("rollback txn/clusteredIndex must not be null");
        }
        return rollbackInternal(txn, clusteredIndex);
    }

    /**
     * 生产 full rollback 入口。每条 undo 都通过 {@link UndoTargetMetadataResolver} 解析自身 table/index target，
     * 因而同一事务可跨多表回滚；无 undo 的只读事务无需 resolver 也可直接进入 ROLLED_BACK。
     */
    public RollbackSummary rollback(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("resolved rollback transaction must not be null");
        }
        if (txn.undoContext() != null && targetResolver == null) {
            throw new DatabaseValidationException("resolved rollback requires UndoTargetMetadataResolver");
        }
        return rollbackInternal(txn, null);
    }

    private RollbackSummary rollbackInternal(Transaction txn, BTreeIndex clusteredIndex) {
        TransactionState initialState = txn.state();
        if (initialState != TransactionState.ACTIVE && initialState != TransactionState.ROLLING_BACK) {
            throw new TransactionStateException(
                    "rollback requires ACTIVE or ROLLING_BACK transaction: " + initialState);
        }
        UndoContext ctx = txn.undoContext();
        if (ctx != null) {
            preflightAllBindings(clusteredIndex, ctx, emptyTargets());
        }
        if (initialState == TransactionState.ACTIVE) {
            txnMgr.beginRollback(txn);
        }

        int applied = 0;
        if (ctx != null) {
            while (true) {
                UndoLogBinding binding = newestBinding(ctx, emptyTargets());
                if (binding == null) {
                    break;
                }
                UndoLogicalHead expectedHead = binding.logicalHead();
                RecordAt current = readUndoRecord(
                        binding.firstPageId(), clusteredIndex, expectedHead.rollPointer());
                UndoLogicalHead targetHead = derivePredecessorHead(
                        binding.firstPageId(), clusteredIndex, expectedHead, current);
                applyUndoRecordInOwnMtr(current, clusteredIndex);
                applied++;
                progressFaultInjector.after(RollbackProgressPhase.AFTER_INVERSE_COMMIT, expectedHead);
                persistLogicalHead(binding.firstPageId(), expectedHead, targetHead, current,
                        clusteredIndex);
                // marker 已提交后只发布无 IO 的内存 pair；测试 hook 放在发布之后，异常也不会制造持久/内存撕裂。
                ctx.publishRollbackProgress(binding.kind(), targetHead);
                progressFaultInjector.after(RollbackProgressPhase.AFTER_PROGRESS_COMMIT, targetHead);
            }
        }
        if (ctx != null) {
            finalizer.finalizeLiveRollback(txn, ctx);
        } else {
            // 无 undo segment 时没有 page3 可交叉校验；终态 delta 是 recovery table 的正式证据，写成功后才能发布内存终态。
            writeRollbackCompleteRedo(txn);
        }
        txnMgr.finishRollback(txn);
        return new RollbackSummary(applied);
    }

    /**
     * 在事务首写前铸造一次性空 undo statement 边界。该令牌绑定本 service 与事务实例，后续只有携带它的
     * rollback/close 才能消费空边界；事务已经创建 {@link UndoContext} 时必须改用真实保存点。
     *
     * @param txn 当前 ACTIVE 且尚未首写的事务。
     * @return 绑定本 service 与事务的一次性空 undo 边界能力。
     */
    public EmptyUndoBoundary createEmptyStatementBoundary(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("empty statement boundary transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException(
                    "empty statement boundary requires ACTIVE transaction: " + txn.state());
        }
        if (txn.undoContext() != null) {
            throw new TransactionStateException("empty statement boundary requires no existing undo context");
        }
        return new EmptyUndoBoundary(this, txn);
    }

    /**
     * statement rollback 发生结果不确定错误时撤销事务提交资格。事务仍保持 ACTIVE 并保留 undo/锁/ReadView，
     * 使调用方能够转入完整事务 rollback；后续写入和提交由 TransactionManager 统一拒绝。
     *
     * @param txn   发生 statement rollback 失败的事务。
     * @param cause 原始失败，首个原因进入事务诊断状态。
     */
    public void markRollbackOnly(Transaction txn, RuntimeException cause) {
        txnMgr.markRollbackOnly(txn, cause);
    }

    /**
     * 创建 storage 内部保存点。v1 保存点挂在已经存在的 {@link UndoContext} 上，因此调用方应在事务首写之后
     * 使用本方法；{@code ClusteredDmlService.beginStatement} 对首写前场景改用专用 empty-boundary Guard，
     * 不伪造一个不属于任何 undo context 的保存点。
     *
     * @param txn 当前 ACTIVE 且已经写过 undo 的事务。
     * @return 绑定到该事务 undo context 的保存点。
     */
    public TransactionSavepoint createSavepoint(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("savepoint transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("createSavepoint requires ACTIVE transaction: " + txn.state());
        }
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            throw new TransactionStateException("createSavepoint requires an existing undo context");
        }
        return ctx.createSavepoint(txn);
    }

    /**
     * 回滚到事务保存点。数据流与完整 rollback 共用单条 undo 反向命令，但不进入事务终态：
     * <ol>
     *   <li>校验事务仍为 ACTIVE，保存点由同一事务创建。</li>
     *   <li>从 INSERT/UPDATE 两个当前局部头按 undoNo 归并向前扫描。</li>
     *   <li>同时精确命中保存点的 roll pointer 与 undoNo；仅应用该边界之后的记录。每条仍用独立 MTR，
     *       失败只回滚当前 MTR 并传播。</li>
     *   <li>全部逆操作成功后，以独立写 MTR compare-and-set 持久 logical head；marker 提交成功后才把
     *       {@link UndoContext} 逻辑链头退回保存点并修剪保存点栈。</li>
     * </ol>
     * 本方法不释放 undo slot、不释放事务级 ReadView、不释放行锁、不写 {@code TRX_STATE_DELTA(ROLLED_BACK)}，
     * 因为数据库事务仍保持 ACTIVE，后续写入会继续追加到同一 undo segment。
     *
     * @param txn            当前 ACTIVE 事务。
     * @param clusteredIndex 该事务写入的聚簇索引（提供 keyDef/schema 解码 undo + 删除/恢复目标）。
     * @param savepoint      目标保存点。
     * @return 本次 partial rollback 实际应用的 undo record 条数。
     */
    public RollbackSummary rollbackToSavepoint(Transaction txn, BTreeIndex clusteredIndex,
                                               TransactionSavepoint savepoint) {
        if (txn == null || clusteredIndex == null || savepoint == null) {
            throw new DatabaseValidationException("rollbackToSavepoint txn/index/savepoint must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("rollbackToSavepoint requires ACTIVE transaction: " + txn.state());
        }
        if (savepoint.transaction() != txn) {
            throw new DatabaseValidationException("savepoint belongs to a different transaction");
        }

        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            return new RollbackSummary(0);
        }
        ctx.requireOwnedSavepoint(savepoint);
        Map<UndoLogKind, UndoLogicalHead> targets = new EnumMap<>(UndoLogKind.class);
        targets.put(UndoLogKind.INSERT, savepoint.insertHead());
        targets.put(UndoLogKind.UPDATE, savepoint.updateHead());
        int applied = rollbackMergedAfter(txn, clusteredIndex, ctx, targets);
        ctx.completeRollbackToSavepoint(savepoint);
        return new RollbackSummary(applied);
    }

    /**
     * 把当前事务回滚到首写前的空 undo 边界。该入口专供 statement guard 在“创建 guard 时事务还没有
     * {@link UndoContext}，但语句执行过程中发生首写”的场景使用；它不会伪造保存点，也不会跳过保存点归属校验。
     *
     * <p>数据流为：校验事务仍为 ACTIVE → 若语句从未写入则返回 0 → 从当前逻辑链头反向应用全部 undo →
     * 独立写 MTR 把已存在的一个或两个 logical heads 同批持久为空，再清空运行期保存点。
     * {@link UndoContext#lastUndoNo()}、undo slot、ReadView、事务行锁和事务状态均保持不变，使事务可以继续写入。
     *
     * @param txn            当前 ACTIVE 事务。
     * @param clusteredIndex 该事务写入的聚簇索引，用于解码和反向应用 undo。
     * @param boundary       本 service 在该事务首写前创建的一次性空边界能力。
     * @return 本次回滚实际应用的 undo record 条数；语句没有写入时为 0。
     */
    public RollbackSummary rollbackToEmptyStatementBoundary(Transaction txn, BTreeIndex clusteredIndex,
                                                            EmptyUndoBoundary boundary) {
        if (txn == null || clusteredIndex == null || boundary == null) {
            throw new DatabaseValidationException("empty statement rollback txn/index/boundary must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException(
                    "empty statement rollback requires ACTIVE transaction: " + txn.state());
        }
        boundary.requireOpen(this, txn);
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            boundary.markRolledBack();
            return new RollbackSummary(0);
        }

        int applied = rollbackMergedAfter(txn, clusteredIndex, ctx, emptyTargets());
        ctx.completeRollbackToEmptyBoundary();
        boundary.markRolledBack();
        return new RollbackSummary(applied);
    }

    /**
     * 按语句成功路径关闭空 undo 边界。它不读取或修改 undo context，只校验事务仍为 ACTIVE 并消费一次性能力；
     * 因而即使语句执行期间已经发生首写，成功 close 也会完整保留当前 undo 链供 commit/full rollback 使用。
     *
     * @param txn      边界所属 ACTIVE 事务。
     * @param boundary 要关闭的一次性空 undo 边界能力。
     */
    public void releaseEmptyStatementBoundary(Transaction txn, EmptyUndoBoundary boundary) {
        if (txn == null || boundary == null) {
            throw new DatabaseValidationException("release empty statement boundary txn/boundary must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException(
                    "release empty statement boundary requires ACTIVE transaction: " + txn.state());
        }
        boundary.requireOpen(this, txn);
        boundary.markClosed();
    }

    /**
     * 释放一个运行期保存点及其嵌套边界，不修改 undo 链。statement guard 在成功路径或 partial rollback 完成后
     * 调用本方法，避免已经离开语句作用域的边界继续留在 {@link UndoContext} 中。事务必须仍为 ACTIVE，且保存点
     * 必须属于该事务当前的 undo context；非法或重复释放会以领域异常暴露调用方生命周期错误。
     *
     * @param txn       保存点所属 ACTIVE 事务。
     * @param savepoint 要释放的运行期保存点。
     */
    public void releaseSavepoint(Transaction txn, TransactionSavepoint savepoint) {
        if (txn == null || savepoint == null) {
            throw new DatabaseValidationException("releaseSavepoint txn/savepoint must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("releaseSavepoint requires ACTIVE transaction: " + txn.state());
        }
        if (savepoint.transaction() != txn) {
            throw new DatabaseValidationException("savepoint belongs to a different transaction");
        }
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            throw new DatabaseValidationException("savepoint transaction has no undo context");
        }
        ctx.releaseSavepoint(savepoint);
    }

    /**
     * 反向应用当前逻辑链中严格晚于指定 undoNo 边界的记录。方法先逐 pointer 使用短只读 MTR 预扫描并精确命中
     * {@code (RollPointer,UndoNo)}；只有边界验证成功后，才从原链头再走一遍，每次用短只读 MTR 物化一条 command、
     * 释放 undo latch，再放进独立写 MTR。这样损坏或陈旧边界不会留下部分 statement rollback，大语句也不会把
     * 整条 undo 页链同时 fixed 在 Buffer Pool。
     *
     * <p>预扫描完成后不再持有 undo page latch；每条反向命令只持聚簇索引所需的短页 latch。单条应用失败时
     * 已成功提交的前序命令仍保持幂等可恢复，但事务会由上层 Guard 标为 rollback-only，禁止提交不确定结果。
     * 本方法不移动 context 链头，只有调用方在整个边界成功到达后才能提交运行期边界状态。
     *
     * @param txn            仅用于明确该链属于哪个事务；事务状态已由公开入口校验。
     * @param clusteredIndex undo 解码和聚簇反向命令所需的索引快照。
     * @param ctx            当前事务的 undo context。
     * @param boundaryUndoNo      边界 undoNo；{@link UndoNo#NONE} 表示走到物理链尾即到达空边界。
     * @param boundaryRollPointer 边界记录的精确指针；空边界必须为 {@link RollPointer#NULL}。
     * @return 实际反向应用的 undo record 数量。
     */
    private int rollbackMergedAfter(Transaction txn, BTreeIndex clusteredIndex, UndoContext ctx,
                                    Map<UndoLogKind, UndoLogicalHead> targets) {
        if (txn.undoContext() != ctx) {
            throw new DatabaseValidationException("rollback undo context is not owned by transaction");
        }
        preflightAllBindings(clusteredIndex, ctx, targets);
        EnumMap<UndoLogKind, UndoLogicalHead> working = new EnumMap<>(UndoLogKind.class);
        EnumMap<UndoLogKind, UndoLogicalHead> original = new EnumMap<>(UndoLogKind.class);
        for (UndoLogBinding binding : ctx.bindings()) {
            working.put(binding.kind(), binding.logicalHead());
            original.put(binding.kind(), binding.logicalHead());
        }
        int applied = 0;
        List<RecordAt> appliedRecords = new ArrayList<>();
        while (true) {
            UndoLogBinding binding = newestBinding(ctx, targets, working);
            if (binding == null) {
                break;
            }
            UndoLogicalHead currentHead = working.get(binding.kind());
            RecordAt at = readUndoRecord(binding.firstPageId(), clusteredIndex,
                    currentHead.rollPointer());
            applyUndoRecordInOwnMtr(at, clusteredIndex);
            applied++;
            appliedRecords.add(at);
            working.put(binding.kind(), derivePredecessorHead(
                    binding.firstPageId(), clusteredIndex, currentHead, at));
        }
        List<HeadUpdate> updates = new ArrayList<>();
        for (UndoLogBinding binding : ctx.bindings()) {
            UndoLogicalHead target = targetFor(targets, binding.kind());
            if (!original.get(binding.kind()).equals(target)) {
                updates.add(new HeadUpdate(binding.firstPageId(), original.get(binding.kind()), target));
            }
        }
        persistLogicalHeads(updates, clusteredIndex, appliedRecords);
        return applied;
    }

    /**
     * 在任何聚簇记录修改前验证目标边界。每个 pointer 都在独立只读 MTR 中读取，提交后立即释放该 undo 页的
     * S latch/fix；因此扫描页数可以超过 Buffer Pool 容量。空边界要求当前链自然走到 NULL；真实保存点要求指针
     * 和 undoNo 同时命中。若 undoNo 已越过目标但指针未命中，说明事务逻辑链损坏，必须拒绝而不能仅凭序号猜测。
     */
    private void preflightAllBindings(BTreeIndex clusteredIndex, UndoContext ctx,
                                      Map<UndoLogKind, UndoLogicalHead> targets) {
        for (UndoLogBinding binding : ctx.bindings()) {
            preflightBinding(clusteredIndex, binding, targetFor(targets, binding.kind()));
        }
    }

    private void preflightBinding(BTreeIndex clusteredIndex, UndoLogBinding binding,
                                  UndoLogicalHead target) {
        UndoLogicalHead persistentHead = readPersistentLogicalHead(binding.firstPageId());
        if (!persistentHead.equals(binding.logicalHead())) {
            throw new UndoLogFormatException("in-memory " + binding.kind() + " logical head "
                    + binding.logicalHead() + " differs from persistent head " + persistentHead);
        }
        RollPointer rp = binding.logicalHead().rollPointer();
        long previousUndoNo = 0L;
        boolean first = true;
        while (!rp.isNull()) {
            RecordAt at = readUndoRecord(binding.firstPageId(), clusteredIndex, rp);
            UndoRecord rec = at.record();
            long undoNo = rec.undoNo().value();
            if (first) {
                if (undoNo != binding.logicalHead().undoNo().value()) {
                    throw new UndoLogFormatException("in-memory logical head undoNo "
                            + binding.logicalHead().undoNo().value() + " resolves to " + undoNo);
                }
            } else if (undoNo >= previousUndoNo) {
                throw new UndoLogFormatException("undo logical chain is not strictly descending: "
                        + undoNo + " after " + previousUndoNo);
            }
            if (!target.isEmpty() && rp.equals(target.rollPointer())) {
                if (!rec.undoNo().equals(target.undoNo())) {
                    throw new UndoLogFormatException(
                            "savepoint roll pointer resolves to a different undo number");
                }
                return;
            } else if (!target.isEmpty() && rec.undoNo().value() <= target.undoNo().value()) {
                // 指针未命中目标却已越过其 undoNo，说明逻辑链断裂；不能把保存点指针重新装回 context。
                throw new UndoLogFormatException(
                        "savepoint roll pointer is not reachable from current undo chain");
            }
            first = false;
            previousUndoNo = undoNo;
            rp = rec.prevRollPointer();
        }
        if (!target.isEmpty()) {
            throw new UndoLogFormatException("savepoint boundary is not reachable from current undo chain");
        }
    }

    private static Map<UndoLogKind, UndoLogicalHead> emptyTargets() {
        EnumMap<UndoLogKind, UndoLogicalHead> targets = new EnumMap<>(UndoLogKind.class);
        targets.put(UndoLogKind.INSERT, UndoLogicalHead.EMPTY);
        targets.put(UndoLogKind.UPDATE, UndoLogicalHead.EMPTY);
        return targets;
    }

    private static UndoLogicalHead targetFor(Map<UndoLogKind, UndoLogicalHead> targets, UndoLogKind kind) {
        if (targets == null || targets.get(kind) == null) {
            throw new DatabaseValidationException("rollback target missing for undo kind " + kind);
        }
        return targets.get(kind);
    }

    private static UndoLogBinding newestBinding(UndoContext ctx,
                                                Map<UndoLogKind, UndoLogicalHead> targets) {
        EnumMap<UndoLogKind, UndoLogicalHead> current = new EnumMap<>(UndoLogKind.class);
        ctx.bindings().forEach(binding -> current.put(binding.kind(), binding.logicalHead()));
        return newestBinding(ctx, targets, current);
    }

    private static UndoLogBinding newestBinding(UndoContext ctx,
                                                Map<UndoLogKind, UndoLogicalHead> targets,
                                                Map<UndoLogKind, UndoLogicalHead> current) {
        UndoLogBinding newest = null;
        long newestUndoNo = 0L;
        for (UndoLogBinding binding : ctx.bindings()) {
            UndoLogicalHead head = current.get(binding.kind());
            if (head.equals(targetFor(targets, binding.kind()))) {
                continue;
            }
            if (head.isEmpty() || head.undoNo().value() <= newestUndoNo) {
                if (!head.isEmpty() && head.undoNo().value() == newestUndoNo) {
                    throw new UndoLogFormatException("independent undo heads share duplicate undoNo " + newestUndoNo);
                }
                continue;
            }
            newest = binding;
            newestUndoNo = head.undoNo().value();
        }
        return newest;
    }

    /**
     * 用独立只读 MTR 物化一条 undo record，返回前释放所有 undo page latch/fix。live、partial 与 recovery 共用
     * 该入口，避免不同回滚路径对 pointer/segment/transaction/index 校验产生漂移。
     */
    private RecordAt readUndoRecord(PageId firstPageId, BTreeIndex clusteredIndex, RollPointer rp) {
        MiniTransaction readMtr = mtrMgr.beginReadOnly();
        try {
            UndoLogSegment seg = undoAccess.open(readMtr, firstPageId, PageLatchMode.SHARED);
            var identity = seg.readRecordIdentity(rp);
            UndoTargetMetadata target = resolveTarget(identity.tableId(), identity.indexId(), clusteredIndex);
            BTreeIndex resolved = target.clusteredIndex();
            UndoRecord record = seg.readRecord(rp, resolved.keyDef(), resolved.schema());
            mtrMgr.commit(readMtr);
            return new RecordAt(record, rp, target);
        } catch (RuntimeException e) {
            if (readMtr.state() == MiniTransactionState.ACTIVE) {
                try {
                    mtrMgr.rollbackUncommitted(readMtr);
                } catch (RuntimeException releaseError) {
                    e.addSuppressed(releaseError);
                }
            }
            throw e;
        }
    }

    /**
     * 在修改聚簇页之前从真实 predecessor record 派生下一持久头。undoNo 不能用算术减一：partial rollback 后新 append
     * 可形成 {@code 3 -> 1}。读取前驱使用独立短 MTR；损坏 pointer、错误归属或非严格下降会在 inverse 前失败。
     */
    private UndoLogicalHead derivePredecessorHead(PageId firstPageId, BTreeIndex clusteredIndex,
                                                   UndoLogicalHead expectedHead, RecordAt current) {
        if (!current.pointer().equals(expectedHead.rollPointer())
                || !current.record().undoNo().equals(expectedHead.undoNo())) {
            throw new UndoLogFormatException("logical head " + expectedHead
                    + " resolves to undoNo=" + current.record().undoNo().value()
                    + " pointer=" + current.pointer());
        }
        RollPointer predecessorPointer = current.record().prevRollPointer();
        if (predecessorPointer.isNull()) {
            return UndoLogicalHead.EMPTY;
        }
        RecordAt predecessor = readUndoRecord(firstPageId, clusteredIndex, predecessorPointer);
        if (predecessor.record().undoNo().value() >= current.record().undoNo().value()) {
            throw new UndoLogFormatException("undo logical predecessor is not strictly lower: current="
                    + current.record().undoNo().value() + ", predecessor="
                    + predecessor.record().undoNo().value());
        }
        return new UndoLogicalHead(predecessor.record().undoNo(), predecessorPointer);
    }

    /** 单条 inverse 的独立 index MTR；异常时只释放当前物理资源，已提交的前序 record 保持可恢复。 */
    private void applyUndoRecordInOwnMtr(RecordAt at, BTreeIndex clusteredIndex) {
        BTreeIndex resolved = at.index();
        MiniTransaction inverseMtr = mtrMgr.begin(mtrMgr.budgetFor(
                RedoBudgetPurpose.ROLLBACK_INVERSE,
                rollbackInverseWorkload(at.record(), resolved)));
        try {
            applyUndoRecord(inverseMtr, at.record(), at.pointer(), resolved);
            mtrMgr.commit(inverseMtr);
        } catch (RuntimeException e) {
            rollbackActiveStateMtr(inverseMtr, e);
            throw e;
        }
    }

    /**
     * 在所有 statement 逆操作成功后，用独立短写 MTR compare-and-set first-page 持久 logical head。marker
     * commit 返回前绝不移动内存 context；stale expected、目标 record 损坏或 MTR 失败都会向上传播，使 Guard
     * 把事务标成 rollback-only。由于 marker 永不领先数据修改，crash 最坏只会让 recovery 幂等重做已完成逆操作。
     */
    private void persistLogicalHead(PageId firstPageId, UndoLogicalHead expectedHead,
                                    UndoLogicalHead targetHead, RecordAt current,
                                    BTreeIndex clusteredIndex) {
        BTreeIndex markerIndex = targetHead.isEmpty()
                ? current.index()
                : resolveIndexAt(firstPageId, targetHead.rollPointer(), clusteredIndex);
        RedoBudgetWorkload markerWorkload = rollbackMarkerWorkload(List.of(current));
        MiniTransaction markerMtr = mtrMgr.begin(
                mtrMgr.budgetFor(RedoBudgetPurpose.ROLLBACK_MARKER, markerWorkload));
        try {
            UndoLogSegment writable = undoAccess.open(
                    markerMtr, firstPageId, PageLatchMode.EXCLUSIVE);
            try (var ignored = markerMtr.allowOutOfOrderPageLatch(
                    "rollback marker owns undo first-page before LOB/FSP; LOB/FSP never waits for undo pages")) {
                freeInsertedLobs(markerMtr, current);
                writable.updateLogicalHead(expectedHead, targetHead,
                        markerIndex.keyDef(), markerIndex.schema());
            }
            mtrMgr.commit(markerMtr);
        } catch (RuntimeException e) {
            rollbackActiveStateMtr(markerMtr, e);
            throw e;
        }
    }

    /**
     * partial rollback 的所有 inverse 完成后，在一个 marker MTR 中按 PageId 排序更新一个或两个 first-page heads。
     * marker 永不领先数据修改；任一 CAS 失败都不发布内存保存点边界，事务由上层转为 rollback-only。
     */
    private void persistLogicalHeads(List<HeadUpdate> updates, BTreeIndex clusteredIndex,
                                     List<RecordAt> appliedRecords) {
        if (updates.isEmpty()) {
            return;
        }
        List<ResolvedHeadUpdate> ordered = updates.stream()
                .map(update -> new ResolvedHeadUpdate(update,
                        update.target().isEmpty() ? requireFallbackIndex(clusteredIndex)
                                : resolveIndexAt(update.firstPageId(), update.target().rollPointer(), clusteredIndex)))
                .sorted(Comparator.comparingInt((ResolvedHeadUpdate item) -> item.firstPageId().spaceId().value())
                        .thenComparingLong(item -> item.firstPageId().pageNo().value()))
                .toList();
        MiniTransaction marker = mtrMgr.begin(mtrMgr.budgetFor(
                RedoBudgetPurpose.ROLLBACK_MARKER, rollbackMarkerWorkload(appliedRecords)));
        try {
            try (var ignored = marker.allowOutOfOrderPageLatch(
                    "dual undo marker: target records may live on non-monotonic chain pages after sorted first pages")) {
                // 先固定全部 first-page X guard，随后才触碰 LOB/FSP；logical head 最后写，不能领先 ownership free。
                List<UndoLogSegment> writableSegments = new ArrayList<>(ordered.size());
                for (ResolvedHeadUpdate resolved : ordered) {
                    writableSegments.add(undoAccess.open(marker, resolved.firstPageId(), PageLatchMode.EXCLUSIVE));
                }
                for (RecordAt applied : appliedRecords) {
                    freeInsertedLobs(marker, applied);
                }
                for (int i = 0; i < ordered.size(); i++) {
                    ResolvedHeadUpdate resolved = ordered.get(i);
                    HeadUpdate update = resolved.update();
                    writableSegments.get(i).updateLogicalHead(update.expected(), update.target(),
                                    resolved.index().keyDef(), resolved.index().schema());
                }
            }
            mtrMgr.commit(marker);
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(marker, error);
            throw error;
        }
    }

    /** marker 固定 6 个页像预算，加每条 INSERT ownership 的 FSP free/PAGE_INIT 上界。 */
    private RedoBudgetWorkload rollbackMarkerWorkload(List<RecordAt> records) {
        if (records == null) {
            throw new DatabaseValidationException("rollback marker records must not be null");
        }
        RedoBudgetWorkload workload = RedoBudgetWorkload.pageImages(6L);
        for (RecordAt at : records) {
            if (at == null) {
                throw new DatabaseValidationException("rollback marker record list contains null");
            }
            for (var ownership : at.record().insertedLobs()) {
                if (lobStorage == null || at.target().lobSegment().isEmpty()) {
                    throw new UndoLogFormatException(
                            "INSERT LOB ownership requires authoritative DD LOB segment during rollback");
                }
                workload = workload.plus(lobStorage.freeWorkload(ownership.value().reference().pageCount()));
            }
        }
        return workload;
    }

    /**
     * 在 marker MTR 中用权威 table binding 校验并释放一条 INSERT undo 的全部 LOB ownership。segment mismatch、
     * schema ordinal/type 漂移或链损坏都会在 logical head 写入前失败，绝不跳过损坏 ownership。
     */
    private void freeInsertedLobs(MiniTransaction marker, RecordAt at) {
        if (at.record().insertedLobs().isEmpty()) {
            return;
        }
        if (lobStorage == null) {
            throw new UndoLogFormatException("LOB rollback storage is not configured");
        }
        var segment = at.target().lobSegment().orElseThrow(() -> new UndoLogFormatException(
                "undo target lacks authoritative LOB segment"));
        for (var ownership : at.record().insertedLobs()) {
            int ordinal = ownership.columnOrdinal();
            if (ordinal < 0 || ordinal >= at.index().schema().columns().size()) {
                throw new UndoLogFormatException("INSERT LOB ownership column ordinal out of schema: " + ordinal);
            }
            lobStorage.free(marker, segment, at.index().schema().column(ordinal).type(), ownership.value());
        }
    }

    /**
     * 写回滚完成正式恢复 redo。它在 slot release / {@code finishRollback} 之前执行，此时事务仍处于
     * ROLLING_BACK，redo record 能记录真实的 from-state；若写入失败，slot 仍由本事务占用，调用方可安全重试。
     * recovery table 消费该 record 重建终态和 counter 高水位；若事务存在 undo slot，恢复仍必须与 page3/header
     * 交叉校验，不能把 logical redo 当成绕过物理状态检查的许可。
     */
    private void writeRollbackCompleteRedo(Transaction txn) {
        MiniTransaction stateMtr = mtrMgr.begin(
                mtrMgr.budgetFor(RedoBudgetPurpose.TRANSACTION_STATE));
        try {
            TransactionStateRedoDeltas.appendRollbackComplete(stateMtr, txn);
            mtrMgr.commit(stateMtr);
        } catch (RuntimeException e) {
            rollbackActiveStateMtr(stateMtr, e);
            throw e;
        }
    }

    private void rollbackActiveStateMtr(MiniTransaction stateMtr, RuntimeException original) {
        if (stateMtr.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrMgr.rollbackUncommitted(stateMtr);
        } catch (RuntimeException rollbackError) {
            original.addSuppressed(rollbackError);
        }
    }

    /**
     * 恢复期回滚一个 ACTIVE undo segment（R 1.2，§14.5）。**无 live {@link Transaction}**：直接从 undo segment
     * first-page 持久 logical head + 显式配置的聚簇索引重建回滚——
     * <ol>
     *   <li>短只读 MTR 读取持久 {@link UndoLogicalHead} 后立即释放 first-page latch。</li>
     *   <li>沿 {@code prevRollPointer} 逐条短读 current 与前驱；首条必须匹配 header，前驱 undoNo 必须严格下降。</li>
     *   <li>每条 inverse 提交后，以独立 marker MTR 后退持久 logical head，再处理下一条。</li>
     * </ol>
     * 由正式 UNDO_ROLLBACK 阶段从 page3 重建出的、且 undo 段 state=ACTIVE 的 slot 调用；不走 Transaction 状态机
     * （前台 {@link #rollback} 才走）。逻辑头到 EMPTY 后，本方法经 finalizer 原子 cache/free/drop segment + CAS 转移 page3 owner，
     * 提交成功后才释放恢复期内存 slot，因此重启不会重新发现已终结段。
     *
     * <p>legacy 模式使用 {@code clusteredIndex}；DD 模式允许其为 null，由 resolver 逐条解析。
     * {@code rec.indexId() != index.indexId()} 抛。<b>幂等</b>：`deleteClustered`/`replaceClustered`/`setClusteredDeleteMark`
     * 未命中即 no-op，故二次崩溃重复回滚安全。
     *
     * @param slotId         page3 扫描恢复出的 slot id。
     * @param firstPageId    ACTIVE 事务的 undo segment 首页（来自恢复重建的 slot）。
     * @param creatorTrxId   恢复 header 中的 creator transaction id，finalization 前再次校验。
     * @param clusteredIndex 显式配置的聚簇索引（提供 keyDef/schema 解码 undo + 删除/恢复目标）。
     * @return 本次回滚应用的 undo record 条数摘要。
     */
    public RollbackSummary rollbackRecovered(UndoSlotId slotId, PageId firstPageId,
                                               TransactionId creatorTrxId, BTreeIndex clusteredIndex) {
        if (slotId == null || firstPageId == null || creatorTrxId == null
                || clusteredIndex == null && indexResolver == null) {
            throw new DatabaseValidationException(
                    "rollbackRecovered slot/firstPage/creator/clusteredIndex must not be null");
        }
        UndoLogKind kind = readRecoveredKind(firstPageId);
        return rollbackRecovered(List.of(new RecoveredUndoLogIdentity(kind, slotId, firstPageId)),
                creatorTrxId, clusteredIndex);
    }

    /** 同一 recovered ACTIVE 事务的一个或两个局部链按全局 undoNo 归并回滚，并一次性终结全部 slots。 */
    public RollbackSummary rollbackRecovered(Collection<RecoveredUndoLogIdentity> logs,
                                              TransactionId creatorTrxId,
                                              BTreeIndex clusteredIndex) {
        if (logs == null || logs.isEmpty() || creatorTrxId == null
                || clusteredIndex == null && indexResolver == null && targetResolver == null) {
            throw new DatabaseValidationException("recovered rollback group fields must not be empty/null");
        }
        List<UndoLogBinding> bindings = new ArrayList<>();
        EnumMap<UndoLogKind, Boolean> kinds = new EnumMap<>(UndoLogKind.class);
        for (RecoveredUndoLogIdentity log : logs) {
            if (log == null || kinds.putIfAbsent(log.kind(), Boolean.TRUE) != null) {
                throw new DatabaseValidationException("recovered rollback group contains null/duplicate kind");
            }
            bindings.add(new UndoLogBinding(log.kind(), log.slotId(), log.firstPageId(),
                    readPersistentLogicalHead(log.firstPageId())));
        }
        for (UndoLogBinding binding : bindings) {
            preflightRecoveredBinding(clusteredIndex, binding);
        }
        int applied = 0;
        while (true) {
            UndoLogBinding binding = newestRecoveredBinding(bindings);
            if (binding == null) {
                break;
            }
            UndoLogicalHead expectedHead = binding.logicalHead();
            RecordAt current = readUndoRecord(binding.firstPageId(), clusteredIndex, expectedHead.rollPointer());
            UndoLogicalHead targetHead = derivePredecessorHead(
                    binding.firstPageId(), clusteredIndex, expectedHead, current);
            applyUndoRecordInOwnMtr(current, clusteredIndex);
            applied++;
            progressFaultInjector.after(RollbackProgressPhase.AFTER_INVERSE_COMMIT, expectedHead);
            persistLogicalHead(binding.firstPageId(), expectedHead, targetHead, current,
                    clusteredIndex);
            binding.publishHead(targetHead);
            progressFaultInjector.after(RollbackProgressPhase.AFTER_PROGRESS_COMMIT, targetHead);
        }
        finalizer.finalizeRecoveredRollback(bindings, creatorTrxId);
        return new RollbackSummary(applied);
    }

    /** recovery 生产入口；每条 undo identity 通过 DD target resolver 定位，不接受 last-index fallback。 */
    public RollbackSummary rollbackRecovered(Collection<RecoveredUndoLogIdentity> logs,
                                              TransactionId creatorTrxId) {
        if (targetResolver == null) {
            throw new DatabaseValidationException("resolved recovered rollback requires target resolver");
        }
        return rollbackRecovered(logs, creatorTrxId, null);
    }

    private void preflightRecoveredBinding(BTreeIndex index, UndoLogBinding binding) {
        RollPointer pointer = binding.logicalHead().rollPointer();
        long previous = Long.MAX_VALUE;
        while (!pointer.isNull()) {
            RecordAt at = readUndoRecord(binding.firstPageId(), index, pointer);
            if (at.record().undoNo().value() >= previous) {
                throw new UndoLogFormatException("recovered local undo chain is not strictly descending");
            }
            previous = at.record().undoNo().value();
            pointer = at.record().prevRollPointer();
        }
    }

    private static UndoLogBinding newestRecoveredBinding(List<UndoLogBinding> bindings) {
        UndoLogBinding newest = null;
        for (UndoLogBinding binding : bindings) {
            if (binding.logicalHead().isEmpty()) {
                continue;
            }
            if (newest != null && binding.logicalHead().undoNo().equals(newest.logicalHead().undoNo())) {
                throw new UndoLogFormatException("recovered undo logs share duplicate head undoNo");
            }
            if (newest == null || binding.logicalHead().undoNo().value()
                    > newest.logicalHead().undoNo().value()) {
                newest = binding;
            }
        }
        return newest;
    }

    private UndoLogKind readRecoveredKind(PageId firstPageId) {
        MiniTransaction read = mtrMgr.beginReadOnly();
        try {
            UndoLogKind kind = undoAccess.open(read, firstPageId, PageLatchMode.SHARED).undoKind();
            mtrMgr.commit(read);
            return kind;
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(read, error);
            throw error;
        }
    }

    /** 用独立只读 MTR 读取持久权威 logical head，供 live preflight 与 recovery 建链，返回前释放 page 资源。 */
    private UndoLogicalHead readPersistentLogicalHead(PageId firstPageId) {
        MiniTransaction readMtr = mtrMgr.beginReadOnly();
        try {
            UndoLogicalHead head = undoAccess.open(readMtr, firstPageId, PageLatchMode.SHARED).logicalHead();
            mtrMgr.commit(readMtr);
            return head;
        } catch (RuntimeException e) {
            rollbackActiveStateMtr(readMtr, e);
            throw e;
        }
    }

    /** 短只读 MTR 已物化并释放 undo 页资源的 (undo record, 其自身 roll pointer) 对。 */
    private BTreeIndex resolveIndexAt(PageId firstPageId, RollPointer pointer, BTreeIndex fallback) {
        MiniTransaction read = mtrMgr.beginReadOnly();
        try {
            var identity = undoAccess.open(read, firstPageId, PageLatchMode.SHARED).readRecordIdentity(pointer);
            BTreeIndex index = resolveTarget(identity.tableId(), identity.indexId(), fallback).clusteredIndex();
            mtrMgr.commit(read);
            return index;
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(read, error);
            throw error;
        }
    }

    private BTreeIndex resolveIndex(long tableId, long indexId, BTreeIndex fallback) {
        BTreeIndex index = indexResolver == null ? requireFallbackIndex(fallback)
                : indexResolver.resolve(tableId, indexId);
        if (index.indexId() != indexId || !index.clustered()) {
            throw new UndoLogFormatException("undo identity resolved wrong/non-clustered index: table="
                    + tableId + " index=" + indexId + " resolved=" + index.indexId());
        }
        return index;
    }

    /** 优先使用权威 DD target resolver；legacy 显式索引路径只产生 empty LOB binding。 */
    private UndoTargetMetadata resolveTarget(long tableId, long indexId, BTreeIndex fallback) {
        UndoTargetMetadata target = targetResolver == null
                ? new UndoTargetMetadata(resolveIndex(tableId, indexId, fallback), java.util.Optional.empty())
                : targetResolver.resolveTarget(tableId, indexId);
        BTreeIndex index = target.clusteredIndex();
        if (index.indexId() != indexId || !index.clustered()) {
            throw new UndoLogFormatException("undo identity resolved wrong/non-clustered target: table="
                    + tableId + " index=" + indexId + " resolved=" + index.indexId());
        }
        return target;
    }

    private static BTreeIndex requireFallbackIndex(BTreeIndex fallback) {
        if (fallback == null) {
            throw new DatabaseValidationException("rollback requires an index resolver or fallback index");
        }
        return fallback;
    }

    private record RecordAt(UndoRecord record, RollPointer pointer, UndoTargetMetadata target) {
        private BTreeIndex index() {
            return target.clusteredIndex();
        }
    }

    private record HeadUpdate(PageId firstPageId, UndoLogicalHead expected, UndoLogicalHead target) {
    }

    private record ResolvedHeadUpdate(HeadUpdate update, BTreeIndex index) {
        PageId firstPageId() {
            return update.firstPageId();
        }
    }

    /**
     * 对单条 undo record 执行反向命令（Command/Template，§11.2 applyUndoRecord）。{@code expectedRollPtr} 传入正在
     * 应用的 roll pointer（= 写入时盖进记录的 DB_ROLL_PTR），与 {@code expectedTrxId} 一起作所有权校验，只改"本事务/
     * 本版本"那一行。
     * <ul>
     *   <li>{@code INSERT_ROW}：按 cluster key 物理删除未提交插入（{@code deleteClustered}）。</li>
     *   <li>{@code UPDATE_ROW}（T1.3e）：用 undo 内全量旧 image 重建 {@link LogicalRecord}（旧列 + 旧隐藏列，
     *       schema 稳定假设：版本=index.schema().schemaVersion()、未删、CONVENTIONAL），{@code replaceClustered}
     *       整记录恢复到更新前版本。</li>
     *   <li>{@code DELETE_MARK}（T1.3f）：{@code setClusteredDeleteMark(deleted=false, newHidden=rec.oldHiddenColumns(),
     *       expected=(rec.transactionId(), rp))} 取消删除标记并还原删除前旧隐藏列（恢复存活版本）。</li>
     * </ul>
     */
    private void applyUndoRecord(MiniTransaction mtr, UndoRecord rec, RollPointer rp, BTreeIndex index) {
        if (rec.indexId() != index.indexId()) {
            throw new DatabaseRuntimeException("undo indexId " + rec.indexId()
                    + " != rollback index " + index.indexId());
        }
        switch (rec.type()) {
            case INSERT_ROW ->
                    btree.deleteClustered(mtr, index, new SearchKey(rec.clusterKey()), rec.transactionId(), rp);
            case UPDATE_ROW -> {
                LogicalRecord oldRecord = new LogicalRecord(index.schema().schemaVersion(),
                        rec.oldColumnValues(), false, RecordType.CONVENTIONAL, rec.oldHiddenColumns());
                btree.replaceClustered(mtr, index, new SearchKey(rec.clusterKey()), oldRecord,
                        rec.transactionId(), rp);
            }
            case DELETE_MARK -> btree.setClusteredDeleteMark(mtr, index, new SearchKey(rec.clusterKey()),
                    false, rec.oldHiddenColumns(), rec.transactionId(), rp);
        }
    }

    /** INSERT inverse 可能沿整棵树 merge/shrink；UPDATE/DELETE_MARK inverse 仅做单叶页等长改写。 */
    private static RedoBudgetWorkload rollbackInverseWorkload(UndoRecord record, BTreeIndex index) {
        if (record == null || index == null) {
            throw new DatabaseValidationException("rollback redo budget record/index must not be null");
        }
        return switch (record.type()) {
            case INSERT_ROW -> BTreeRedoBudgetEstimator.structuralDelete(index.rootLevel());
            case UPDATE_ROW, DELETE_MARK -> BTreeRedoBudgetEstimator.pointRewrite();
        };
    }
}
