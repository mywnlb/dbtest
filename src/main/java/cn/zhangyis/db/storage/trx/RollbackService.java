package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.BTreeSecondaryDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeSecondaryRemovalResult;
import cn.zhangyis.db.storage.btree.SecondaryDeleteMarkStatus;
import cn.zhangyis.db.storage.btree.SecondaryEntryRemovalStatus;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.api.lob.LobStorage;
import cn.zhangyis.db.storage.api.lob.LobFreeBatchPlan;
import cn.zhangyis.db.storage.api.lob.LobFreeTarget;
import cn.zhangyis.db.storage.api.SegmentRef;
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
import cn.zhangyis.db.storage.undo.SecondaryEntryBeforeState;
import cn.zhangyis.db.storage.undo.SecondaryUndoMutation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collection;
import java.util.Optional;

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
 * <p><b>索引解析</b>：legacy 调用继续显式传单聚簇索引；DatabaseEngine 注入表级
 * {@link UndoTargetMetadataResolver} 后，每条 undo 先读取固定前缀 tableId/indexId，再解析 exact-version
 * 聚簇/secondary layout 与可选 LOB owner。带 secondary tail 的记录按 index id 逐棵树使用独立短 MTR 反向执行，
 * secondary 全部收敛后才修改聚簇版本，最后另用 marker MTR 推进持久 logical head。命名 SAVEPOINT 与
 * savepoint 后 row-lock 精细释放仍留后续。
 */
public final class RollbackService {

    /** rollback 等待短物理行 guard 的上界；等待发生时不持有 MTR、page latch 或 buffer fix。 */
    private static final Duration ROW_GUARD_TIMEOUT = Duration.ofSeconds(30);

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
    /** 结构型二级 inverse 在 redo admission 前刷新 root level；legacy 单索引模式允许为空。 */
    private final BTreeRootSnapshotService rootSnapshots;
    /** 与表级 DML/purge 共享的短物理行协调器；legacy 单索引模式允许为空。 */
    private final PurgeDmlRowGuardManager rowGuards;

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
                null, null, null, null, null);
    }

    /** DD 模式构造器：rollback/recovery 逐条解析 undo 的 tableId/indexId。
     *
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoAccess 由组合根提供的 {@code UndoLogSegmentAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param txnMgr 由组合根提供的 {@code TransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtrMgr 由组合根提供的 {@code MiniTransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param finalizer 由组合根提供的 {@code UndoSegmentFinalizer} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param indexResolver 由组合根提供的 {@code IndexMetadataResolver} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                           UndoSegmentFinalizer finalizer, IndexMetadataResolver indexResolver) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, RollbackProgressFaultInjector.none(),
                indexResolver, null, null, null, null);
    }

    /** DD 生产模式：full/recovery rollback 同时解析精确聚簇索引与权威 LOB segment。
     *
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoAccess 由组合根提供的 {@code UndoLogSegmentAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param txnMgr 由组合根提供的 {@code TransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtrMgr 由组合根提供的 {@code MiniTransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param finalizer 由组合根提供的 {@code UndoSegmentFinalizer} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lobStorage 由组合根提供的 {@code LobStorage} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param targetResolver 由组合根提供的 {@code UndoTargetMetadataResolver} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                           UndoSegmentFinalizer finalizer, LobStorage lobStorage,
                           UndoTargetMetadataResolver targetResolver) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, RollbackProgressFaultInjector.none(),
                null, targetResolver, lobStorage, null, null);
    }

    /**
     * DD 表级生产模式：full/statement/recovery rollback 使用 exact-version 全索引聚合，并与前台 DML 共用行 guard。
     *
     * @param btree          聚簇与二级 inverse 的页结构入口。
     * @param undoAccess     undo logical chain 读取入口。
     * @param txnMgr         live transaction 状态机。
     * @param mtrMgr         每棵索引独立短 MTR 的来源。
     * @param finalizer      完整回滚后终结 undo segment 的协作者。
     * @param lobStorage     INSERT ownership marker 的 LOB 释放入口。
     * @param targetResolver 按 undo table/index identity 解析 exact-version 表级目标。
     * @param rootSnapshots  从 root 页头刷新结构 level 的服务。
     * @param rowGuards      与表级 DML/purge 共享的聚簇行短物理协调器。
     */
    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                           UndoSegmentFinalizer finalizer, LobStorage lobStorage,
                           UndoTargetMetadataResolver targetResolver,
                           BTreeRootSnapshotService rootSnapshots,
                           PurgeDmlRowGuardManager rowGuards) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, RollbackProgressFaultInjector.none(),
                null, targetResolver, lobStorage, rootSnapshots, rowGuards);
    }

    /**
     * 包内测试构造器。只有同包 crash-point 测试可以替换 injector；生产组合根继续调用五参数公开构造器。
     *
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoAccess 由组合根提供的 {@code UndoLogSegmentAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param txnMgr 由组合根提供的 {@code TransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtrMgr 由组合根提供的 {@code MiniTransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param finalizer 由组合根提供的 {@code UndoSegmentFinalizer} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param progressFaultInjector 由当前模块组合根提供的领域协作者；不得为 {@code null}，其状态和生命周期必须覆盖本次调用且不能绕过模块边界
     */
    RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                    TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                    UndoSegmentFinalizer finalizer, RollbackProgressFaultInjector progressFaultInjector) {
        this(btree, undoAccess, txnMgr, mtrMgr, finalizer, progressFaultInjector,
                null, null, null, null, null);
    }

    /**
     * 复制当前生产 wiring 并只替换逐记录 crash hook。该包内接缝让故障测试复用真实 DD target 与 LOB storage，
     * 不修改原实例，也不允许上层把 injector 当成运行期配置；正常组合根始终持有 no-op 实例。
     *
     * @param injector 仅在已提交的 inverse/progress 边界后触发的故障注入器。
     * @return 与当前实例共享无状态协作者、但使用指定 crash hook 的独立回滚执行器。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    RollbackService withProgressFaultInjectorForTest(RollbackProgressFaultInjector injector) {
        if (injector == null) {
            throw new DatabaseValidationException("rollback progress fault injector must not be null");
        }
        return new RollbackService(btree, undoAccess, txnMgr, mtrMgr, finalizer, injector,
                indexResolver, targetResolver, lobStorage, rootSnapshots, rowGuards);
    }

    /**
     * 创建 {@code RollbackService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoAccess 由组合根提供的 {@code UndoLogSegmentAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param txnMgr 由组合根提供的 {@code TransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtrMgr 由组合根提供的 {@code MiniTransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param finalizer 由组合根提供的 {@code UndoSegmentFinalizer} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param progressFaultInjector 由当前模块组合根提供的领域协作者；不得为 {@code null}，其状态和生命周期必须覆盖本次调用且不能绕过模块边界
     * @param indexResolver 由组合根提供的 {@code IndexMetadataResolver} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param targetResolver 由组合根提供的 {@code UndoTargetMetadataResolver} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lobStorage 由组合根提供的 {@code LobStorage} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param rootSnapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param rowGuards 调用方持有的 {@code PurgeDmlRowGuardManager} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                    TransactionManager txnMgr, MiniTransactionManager mtrMgr,
                     UndoSegmentFinalizer finalizer, RollbackProgressFaultInjector progressFaultInjector,
                     IndexMetadataResolver indexResolver, UndoTargetMetadataResolver targetResolver,
                     LobStorage lobStorage, BTreeRootSnapshotService rootSnapshots,
                     PurgeDmlRowGuardManager rowGuards) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (btree == null || undoAccess == null || txnMgr == null || mtrMgr == null
                || finalizer == null || progressFaultInjector == null) {
            throw new DatabaseValidationException("rollback service collaborators must not be null");
        }
        this.btree = btree;
        this.undoAccess = undoAccess;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.txnMgr = txnMgr;
        this.mtrMgr = mtrMgr;
        this.finalizer = finalizer;
        this.progressFaultInjector = progressFaultInjector;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.indexResolver = indexResolver;
        this.targetResolver = targetResolver;
        this.lobStorage = lobStorage;
        this.rootSnapshots = rootSnapshots;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.rowGuards = rowGuards;
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @return {@code rollback} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /**
     * 校验当前状态后推进事务、MVCC 与锁状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code rollbackInternal} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private RollbackSummary rollbackInternal(Transaction txn, BTreeIndex clusteredIndex) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        TransactionState initialState = txn.state();
        if (initialState != TransactionState.ACTIVE && initialState != TransactionState.ROLLING_BACK) {
            throw new TransactionStateException(
                    "rollback requires ACTIVE or ROLLING_BACK transaction: " + initialState);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoContext ctx = txn.undoContext();
        if (ctx != null) {
            preflightAllBindings(clusteredIndex, ctx, emptyTargets());
        }
        if (initialState == TransactionState.ACTIVE) {
            txnMgr.beginRollback(txn);
        }

        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int applied = ctx == null ? 0 : rollbackAllBindings(ctx, clusteredIndex);
        if (ctx != null) {
            finalizer.finalizeLiveRollback(txn, ctx);
        } else {
            // 无 undo segment 时没有 page3 可交叉校验；终态 delta 是 recovery table 的正式证据，写成功后才能发布内存终态。
            writeRollbackCompleteRedo(txn);
        }
        txnMgr.finishRollback(txn);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new RollbackSummary(applied);
    }

    /**
     * 显式回滚 prepared transaction。该入口与普通 rollback 分离，确保 first-page 期望状态和 redo reason
     * 始终保持 PREPARED 语义。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>要求 PREPARED 或可重试 PREPARED_ROLLING_BACK，并预检全部持久 logical chain。</li>
     *     <li>首次调用发布 PREPARED_ROLLING_BACK，但保留 active-table身份和事务锁。</li>
     *     <li>复用逐条 inverse + marker 流程，把一或两条 logical head幂等推进到 EMPTY。</li>
     *     <li>prepared finalizer同批 drop owner、clear page3并写终态 redo，随后 manager移出 active table。</li>
     * </ol>
     *
     * @param txn 待决议的 PREPARED 或失败重试 PREPARED_ROLLING_BACK 事务
     * @param clusteredIndex legacy 单聚簇 metadata；DD resolver模式仍会按每条 undo identity解析
     * @return 本次实际应用的 undo record数量
     * @throws TransactionStateException 事务状态或 undo context不符合 prepared rollback条件时抛出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSummary rollbackPrepared(Transaction txn, BTreeIndex clusteredIndex) {
        if (txn == null || clusteredIndex == null) {
            throw new DatabaseValidationException(
                    "prepared rollback transaction/index must not be null");
        }
        return rollbackPreparedInternal(txn, clusteredIndex, false);
    }

    /**
     * DD resolver 模式 prepared rollback；同一事务可跨多表，不接受 last-index fallback。
     *
     * @param txn 待决议 prepared transaction
     * @return 本次应用的 undo record数量
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSummary rollbackPrepared(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException(
                    "resolved prepared rollback transaction must not be null");
        }
        if (targetResolver == null) {
            throw new DatabaseValidationException(
                    "resolved prepared rollback requires UndoTargetMetadataResolver");
        }
        return rollbackPreparedInternal(txn, null, false);
    }

    /**
     * 启动恢复专用 PREPARED 回滚；与 live 入口共享终态协议，但允许隔离目标仅推进系统 undo logical head。
     *
     * @param txn 从持久 PREPARED slot 恢复出的事务
     * @param clusteredIndex legacy 单索引兼容目标；DD resolver 模式可为 {@code null}
     * @return 健康 inverse 与隔离记录跳过数
     */
    public RollbackSummary rollbackPreparedRecovered(Transaction txn, BTreeIndex clusteredIndex) {
        return rollbackPreparedInternal(txn, clusteredIndex, true);
    }

    /** prepared rollback 两种 metadata入口共用实现。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code rollbackPreparedInternal} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private RollbackSummary rollbackPreparedInternal(Transaction txn, BTreeIndex clusteredIndex,
                                                      boolean recovered) {
        // 1、重试态显式保留 prepared origin；普通 ROLLING_BACK 不能进入。
        TransactionState initialState = txn.state();
        if (initialState != TransactionState.PREPARED
                && initialState != TransactionState.PREPARED_ROLLING_BACK) {
            throw new TransactionStateException(
                    "prepared rollback requires PREPARED or PREPARED_ROLLING_BACK: " + initialState);
        }
        UndoContext context = txn.undoContext();
        if (context == null || context.bindings().isEmpty()) {
            throw new TransactionStateException(
                    "prepared rollback requires ordinary undo context");
        }
        if (recovered) {
            for (UndoLogBinding binding : context.bindings()) {
                preflightRecoveredBinding(clusteredIndex, binding);
            }
        } else {
            preflightAllBindings(clusteredIndex, context, emptyTargets());
        }
        // 2、只在首次决议时推进运行态；失败重试保持原态。
        if (initialState == TransactionState.PREPARED) {
            txnMgr.beginPreparedRollback(txn);
        }
        // 3、marker 更新不依赖 ACTIVE/PREPARED状态，只对持久 logical head做CAS。
        RollbackSummary progress = recovered
                ? rollbackRecoveredBindings(context.bindings(), clusteredIndex)
                : new RollbackSummary(rollbackAllBindings(context, clusteredIndex));
        // 4、物理 owner终结成功后才允许 manager发布 ROLLED_BACK。
        finalizer.finalizePreparedRollback(txn, context);
        txnMgr.finishPreparedRollback(txn);
        return progress;
    }

    /** 普通与 prepared full rollback 共用的逐条 inverse/marker 循环；终结状态由各自调用方处理。
     *
     * @param context 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code rollbackAllBindings} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     */
    private int rollbackAllBindings(UndoContext context, BTreeIndex clusteredIndex) {
        int applied = 0;
        while (true) {
            UndoLogBinding binding = newestBinding(context, emptyTargets());
            if (binding == null) {
                return applied;
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
            context.publishRollbackProgress(binding.kind(), targetHead);
            progressFaultInjector.after(RollbackProgressPhase.AFTER_PROGRESS_COMMIT, targetHead);
        }
    }

    /**
     * 在事务首写前铸造一次性空 undo statement 边界。该令牌绑定本 service 与事务实例，后续只有携带它的
     * rollback/close 才能消费空边界；事务已经创建 {@link UndoContext} 时必须改用真实保存点。
     *
     * @param txn 当前 ACTIVE 且尚未首写的事务。
     * @return 绑定本 service 与事务的一次性空 undo 边界能力。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public RollbackSummary rollbackToSavepoint(Transaction txn, BTreeIndex clusteredIndex,
                                               TransactionSavepoint savepoint) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null || savepoint == null
                || clusteredIndex == null && indexResolver == null && targetResolver == null) {
            throw new DatabaseValidationException("rollbackToSavepoint txn/index/savepoint must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("rollbackToSavepoint requires ACTIVE transaction: " + txn.state());
        }
        if (savepoint.transaction() != txn) {
            throw new DatabaseValidationException("savepoint belongs to a different transaction");
        }

        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            return new RollbackSummary(0);
        }
        ctx.requireOwnedSavepoint(savepoint);
        Map<UndoLogKind, UndoLogicalHead> targets = new EnumMap<>(UndoLogKind.class);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        targets.put(UndoLogKind.INSERT, savepoint.insertHead());
        targets.put(UndoLogKind.UPDATE, savepoint.updateHead());
        int applied = rollbackMergedAfter(txn, clusteredIndex, ctx, targets);
        ctx.completeRollbackToSavepoint(savepoint);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new RollbackSummary(applied);
    }

    /**
     * 生产 DD resolver 模式的命名保存点入口。每条 undo 自带 table/index identity，本入口不接受 Session
     * 传入最后访问的索引，避免跨表事务回滚时误用单表 fallback。
     *
     * @param txn 当前 ACTIVE 事务
     * @param savepoint 同一事务当前 undo context 拥有的保存点
     * @return 实际反向应用的 undo 记录数
     * @throws DatabaseValidationException 当前组合根没有 target/index resolver 或保存点归属错误时抛出
     */
    public RollbackSummary rollbackToSavepoint(Transaction txn, TransactionSavepoint savepoint) {
        if (targetResolver == null && indexResolver == null) {
            throw new DatabaseValidationException(
                    "resolved savepoint rollback requires target or index resolver");
        }
        return rollbackToSavepoint(txn, null, savepoint);
    }

    /**
     * 把当前事务回滚到首写前的空 undo 边界。该入口专供 statement guard 在“创建 guard 时事务还没有
     * {@link UndoContext}，但语句执行过程中发生首写”的场景使用；它不会伪造保存点，也不会跳过保存点归属校验。
     *
     * <p>数据流为：校验事务仍为 ACTIVE → 若语句从未写入则返回 0 → 从当前逻辑链头反向应用全部 undo →
     * 独立写 MTR 把已存在的一个或两个 logical heads 同批持久为空，再清空运行期保存点。
     * {@link UndoContext#lastUndoNo()}、undo slot、ReadView、事务行锁和事务状态均保持不变，使事务可以继续写入。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn            当前 ACTIVE 事务。
     * @param clusteredIndex 该事务写入的聚簇索引，用于解码和反向应用 undo。
     * @param boundary       本 service 在该事务首写前创建的一次性空边界能力。
     * @return 本次回滚实际应用的 undo record 条数；语句没有写入时为 0。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public RollbackSummary rollbackToEmptyStatementBoundary(Transaction txn, BTreeIndex clusteredIndex,
                                                            EmptyUndoBoundary boundary) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null || boundary == null
                || clusteredIndex == null && indexResolver == null && targetResolver == null) {
            throw new DatabaseValidationException("empty statement rollback txn/index/boundary must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException(
                    "empty statement rollback requires ACTIVE transaction: " + txn.state());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        boundary.requireOpen(this, txn);
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            boundary.markRolledBack();
            return new RollbackSummary(0);
        }

        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int applied = rollbackMergedAfter(txn, clusteredIndex, ctx, emptyTargets());
        ctx.completeRollbackToEmptyBoundary();
        boundary.markRolledBack();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new RollbackSummary(applied);
    }

    /**
     * 使用生产 DD resolver 把命名保存点回滚到首写前空边界；适用于保存点创建后首次写入任意表的场景。
     *
     * @param txn 当前 ACTIVE 事务
     * @param boundary 本 service 在事务首写前创建的空边界能力
     * @return 实际反向应用的 undo 记录数
     */
    public RollbackSummary rollbackToEmptyStatementBoundary(
            Transaction txn, EmptyUndoBoundary boundary) {
        if (targetResolver == null && indexResolver == null) {
            throw new DatabaseValidationException(
                    "resolved empty-boundary rollback requires target or index resolver");
        }
        return rollbackToEmptyStatementBoundary(txn, null, boundary);
    }

    /**
     * 按语句成功路径关闭空 undo 边界。它不读取或修改 undo context，只校验事务仍为 ACTIVE 并消费一次性能力；
     * 因而即使语句执行期间已经发生首写，成功 close 也会完整保留当前 undo 链供 commit/full rollback 使用。
     *
     * @param txn      边界所属 ACTIVE 事务。
     * @param boundary 要关闭的一次性空 undo 边界能力。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
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
     * 释放一个运行期保存点，不修改 undo 链或更晚边界。statement guard 在成功路径或 partial rollback 完成后
     * 调用本方法，避免已经离开语句作用域的边界继续留在 {@link UndoContext} 中。事务必须仍为 ACTIVE，且保存点
     * 必须属于该事务当前的 undo context；非法或重复释放会以领域异常暴露调用方生命周期错误。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn       保存点所属 ACTIVE 事务。
     * @param savepoint 要释放的运行期保存点。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void releaseSavepoint(Transaction txn, TransactionSavepoint savepoint) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null || savepoint == null) {
            throw new DatabaseValidationException("releaseSavepoint txn/savepoint must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("releaseSavepoint requires ACTIVE transaction: " + txn.state());
        }
        if (savepoint.transaction() != txn) {
            throw new DatabaseValidationException("savepoint belongs to a different transaction");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            throw new DatabaseValidationException("savepoint transaction has no undo context");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
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
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn            仅用于明确该链属于哪个事务；事务状态已由公开入口校验。
     * @param clusteredIndex undo 解码和聚簇反向命令所需的索引快照。
     * @param ctx            当前事务的 undo context。
     * @param boundaryUndoNo      边界 undoNo；{@link UndoNo#NONE} 表示走到物理链尾即到达空边界。
     * @param boundaryRollPointer 边界记录的精确指针；空边界必须为 {@link RollPointer#NULL}。
     * @return 实际反向应用的 undo record 数量。
     * @param targets 参与 {@code rollbackMergedAfter} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private int rollbackMergedAfter(Transaction txn, BTreeIndex clusteredIndex, UndoContext ctx,
                                    Map<UndoLogKind, UndoLogicalHead> targets) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn.undoContext() != ctx) {
            throw new DatabaseValidationException("rollback undo context is not owned by transaction");
        }
        preflightAllBindings(clusteredIndex, ctx, targets);
        EnumMap<UndoLogKind, UndoLogicalHead> working = new EnumMap<>(UndoLogKind.class);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        EnumMap<UndoLogKind, UndoLogicalHead> original = new EnumMap<>(UndoLogKind.class);
        for (UndoLogBinding binding : ctx.bindings()) {
            working.put(binding.kind(), binding.logicalHead());
            original.put(binding.kind(), binding.logicalHead());
        }
        int applied = 0;
        List<RecordAt> appliedRecords = new ArrayList<>();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
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
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
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

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param binding 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param target 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void preflightBinding(BTreeIndex clusteredIndex, UndoLogBinding binding,
                                  UndoLogicalHead target) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        UndoLogicalHead persistentHead = readPersistentLogicalHead(binding.firstPageId());
        if (!persistentHead.equals(binding.logicalHead())) {
            throw new UndoLogFormatException("in-memory " + binding.kind() + " logical head "
                    + binding.logicalHead() + " differs from persistent head " + persistentHead);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        RollPointer rp = binding.logicalHead().rollPointer();
        long previousUndoNo = 0L;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        boolean first = true;
        while (!rp.isNull()) {
            RecordAt at = readUndoRecord(binding.firstPageId(), clusteredIndex, rp);
            requireLiveTargetAvailable(at);
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
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
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

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * @param ctx 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param targets 参与 {@code newestBinding} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @return {@code newestBinding} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    private static UndoLogBinding newestBinding(UndoContext ctx,
                                                Map<UndoLogKind, UndoLogicalHead> targets) {
        EnumMap<UndoLogKind, UndoLogicalHead> current = new EnumMap<>(UndoLogKind.class);
        ctx.bindings().forEach(binding -> current.put(binding.kind(), binding.logicalHead()));
        return newestBinding(ctx, targets, current);
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param ctx 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param targets 参与 {@code newestBinding} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @param current 参与 {@code newestBinding} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @return {@code newestBinding} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static UndoLogBinding newestBinding(UndoContext ctx,
                                                Map<UndoLogKind, UndoLogicalHead> targets,
                                                Map<UndoLogKind, UndoLogicalHead> current) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        UndoLogBinding newest = null;
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        long newestUndoNo = 0L;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
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
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return newest;
    }

    /**
     * 用独立只读 MTR 物化一条 undo record，返回前释放所有 undo page latch/fix。live、partial 与 recovery 共用
     * 该入口，避免不同回滚路径对 pointer/segment/transaction/index 校验产生漂移。
     *
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param rp 参与 {@code readUndoRecord} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code readUndoRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
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

    /**
     * 撤销一条逻辑 undo 对聚簇树和全部二级树的影响；每棵树使用独立短 MTR，聚簇 inverse 固定最后执行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>无 secondary tail 时沿用单聚簇兼容路径；表级 tail 则先校验共享 root/guard wiring。</li>
     *     <li>不持任何 MTR/page latch 时取得 table+cluster-key 行 guard，再用短读 MTR物化当前聚簇版本。</li>
     *     <li>若聚簇 inverse 已完成，利用“聚簇最后提交”不变量把整条数据 inverse 判为完成；否则校验当前版本仍由本 undo 拥有。</li>
     *     <li>按 undo 中稳定 index-id 顺序逐棵恢复二级 entry；每个状态转换单独提交，失败后 marker 保持原位供重试。</li>
     *     <li>全部二级树收敛后才提交聚簇 inverse；调用方随后另用 marker MTR 后退 logical head。</li>
     * </ol>
     *
     * @param at             已从 undo 页完整物化的 record、pointer 与 exact-version 表级目标。
     * @param clusteredIndex legacy 显式聚簇索引；表级 resolver 模式使用 {@code at.target()}，该参数仅作兼容。
     * @throws UndoLogFormatException 当前聚簇版本所有权、二级前态或 exact-version metadata 与 undo 证据冲突时抛出。
     */
    private void applyUndoRecordInOwnMtr(RecordAt at, BTreeIndex clusteredIndex) {
        requireLiveTargetAvailable(at);
        // 1. 旧 undo 没有二级 tail，保持已有单树行为，也让低层测试不必伪造表级 wiring。
        if (at.record().secondaryMutations().isEmpty()) {
            applyClusteredInverseInOwnMtr(at);
            return;
        }
        requireMultiIndexRollbackWiring();

        UndoRecord record = at.record();
        SearchKey clusterKey = new SearchKey(record.clusterKey());

        // 2. guard 等待发生在所有短 MTR 之外；拿到 guard 后，各次页访问仍严格一次只进入一棵树。
        try (PurgeDmlRowGuard ignored = rowGuards.acquireForDml(
                record.tableId(), clusterKey, ROW_GUARD_TIMEOUT)) {
            Optional<LogicalRecord> current = readClusteredRecord(at.index(), clusterKey);

            // 3. 聚簇 inverse 固定最后提交，因此它的完成态也是全部 secondary inverse 已完成的重试证明。
            if (clusteredInverseAlreadyComplete(record, at.index(), current)) {
                return;
            }
            LogicalRecord ownedCurrent = requireUndoOwnedCurrentRecord(record, at.pointer(), current);

            // 4. mutation 在 UndoRecord 构造/解码时已按 index id 严格递增；每个动作内部也不跨 MTR 持页资源。
            for (SecondaryUndoMutation mutation : record.secondaryMutations()) {
                applySecondaryInverse(at, mutation, ownedCurrent);
            }

            // 5. 最后恢复/删除聚簇版本；若后续 marker 失败，重试会由第 3 阶段直接跳过已完成的数据 inverse。
            applyClusteredInverseInOwnMtr(at);
        }
    }

    /** 在独立短 MTR 内执行聚簇 inverse；异常时释放当前 MTR 资源且不移动 undo logical head。
     *
     * @param at 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     */
    private void applyClusteredInverseInOwnMtr(RecordAt at) {
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
     * 短读当前聚簇物理版本并在返回前释放页资源；delete-marked 版本也必须返回，以便 DELETE rollback 校验 owner。
     *
     * @param index      exact-version 聚簇 descriptor。
     * @param clusterKey 完整物化聚簇主键。
     * @return 当前物理记录快照；聚簇 INSERT inverse 已完成时为空。
     */
    private Optional<LogicalRecord> readClusteredRecord(BTreeIndex index, SearchKey clusterKey) {
        MiniTransaction read = mtrMgr.beginReadOnly();
        try {
            Optional<LogicalRecord> result = btree.lookupIncludingDeleted(read, index, clusterKey)
                    .map(BTreeLookupResult::record);
            mtrMgr.commit(read);
            return result;
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(read, error);
            throw error;
        }
    }

    /**
     * 判断 marker 落后时当前聚簇行是否已经处于 undo 的目标旧状态。聚簇 inverse 最后提交，所以命中该状态时，
     * earlier secondary inverse 必然都已提交；这里不能再次从旧聚簇 image 猜测 INSERT 的新二级 entry。
     */
    private static boolean clusteredInverseAlreadyComplete(UndoRecord record, BTreeIndex index,
                                                            Optional<LogicalRecord> current) {
        if (record.type() == cn.zhangyis.db.storage.undo.UndoRecordType.INSERT_ROW) {
            return current.isEmpty();
        }
        return current.filter(expectedOldRecord(record, index)::equals).isPresent();
    }

    /**
     * 校验当前聚簇版本仍是本 undo 要撤销的前向版本。所有权由 DB_TRX_ID+DB_ROLL_PTR 双重确定，delete 位还需与
     * undo 类型一致；任一错配都 fail-closed，避免用旧 history 覆盖较新已提交版本。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param pointer 参与 {@code requireUndoOwnedCurrentRecord} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param current 可选的 {@code current}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @return {@code requireUndoOwnedCurrentRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static LogicalRecord requireUndoOwnedCurrentRecord(UndoRecord record, RollPointer pointer,
                                                                Optional<LogicalRecord> current) {
        LogicalRecord row = current.orElseThrow(() -> new UndoLogFormatException(
                "rollback current clustered record is absent before inverse: table=" + record.tableId()
                        + " index=" + record.indexId() + " key=" + record.clusterKey()));
        boolean expectedDeleted = record.type() == cn.zhangyis.db.storage.undo.UndoRecordType.DELETE_MARK;
        if (row.hiddenColumns() == null
                || !record.transactionId().equals(row.hiddenColumns().dbTrxId())
                || !pointer.equals(row.hiddenColumns().dbRollPtr())
                || row.deleted() != expectedDeleted) {
            throw new UndoLogFormatException("rollback clustered owner/state mismatch: table="
                    + record.tableId() + " index=" + record.indexId() + " key=" + record.clusterKey()
                    + " expectedTrx=" + record.transactionId() + " expectedRollPtr=" + pointer
                    + " expectedDeleted=" + expectedDeleted);
        }
        return row;
    }

    /** 从 UPDATE/DELETE undo 的全量旧 image 重建聚簇目标版本；INSERT 没有旧版本，不得调用。 */
    private static LogicalRecord expectedOldRecord(UndoRecord record, BTreeIndex index) {
        if (record.oldColumnValues() == null || record.oldHiddenColumns() == null) {
            throw new UndoLogFormatException(record.type() + " undo has no old clustered image");
        }
        return new LogicalRecord(index.schema().schemaVersion(), record.oldColumnValues(), false,
                RecordType.CONVENTIONAL, record.oldHiddenColumns());
    }

    /**
     * 对一个 exact-version 二级索引应用反向动作。UPDATE 先 revive 旧 identity，再按落盘前态 remove/re-mark 新 identity；
     * 即使阶段间 crash，重试也可依赖显式 CHANGED/ALREADY/ABSENT 状态收敛。
     *
     * @param at 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param mutation 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param currentRow 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     */
    private void applySecondaryInverse(RecordAt at, SecondaryUndoMutation mutation,
                                       LogicalRecord currentRow) {
        SecondaryIndexMetadata secondary = at.target().tableIndexes().requireSecondary(mutation.indexId());
        LogicalRecord currentEntry = secondary.layout().toEntry(currentRow, false);
        switch (mutation.action()) {
            case INSERT_ENTRY -> {
                removePublishedSecondary(secondary, secondary.layout().physicalKey(currentEntry), true);
                notifySecondaryInverseCommitted(at);
            }
            case CHANGE_KEY -> {
                LogicalRecord oldRow = expectedOldRecord(at.record(), at.index());
                LogicalRecord oldEntry = secondary.layout().toEntry(oldRow, false);
                reviveSecondary(secondary, secondary.layout().physicalKey(oldEntry));
                notifySecondaryInverseCommitted(at);
                SearchKey currentKey = secondary.layout().physicalKey(currentEntry);
                if (mutation.newEntryBeforeState() == SecondaryEntryBeforeState.ABSENT) {
                    removePublishedSecondary(secondary, currentKey, true);
                } else if (mutation.newEntryBeforeState() == SecondaryEntryBeforeState.DELETE_MARKED) {
                    markSecondaryDeleted(secondary, currentKey);
                } else {
                    throw secondaryInverseMismatch(at.record(), mutation,
                            "CHANGE_KEY has no recoverable new-entry before-state");
                }
                notifySecondaryInverseCommitted(at);
            }
            case DELETE_MARK_ENTRY -> {
                reviveSecondary(secondary, secondary.layout().physicalKey(currentEntry));
                notifySecondaryInverseCommitted(at);
            }
        }
    }

    /** 在单个 secondary MTR durable 后触发包内故障接缝；logical head 仍保持当前 undo，生产实例为 no-op。 */
    private void notifySecondaryInverseCommitted(RecordAt at) {
        progressFaultInjector.after(RollbackProgressPhase.AFTER_SECONDARY_INVERSE_COMMIT,
                new UndoLogicalHead(at.record().undoNo(), at.pointer()));
    }

    /** 物理删除 rollback 新发布的 live entry；ABSENT 是 marker-lag/阶段重试的幂等完成态，状态冲突必须失败。 */
    private void removePublishedSecondary(SecondaryIndexMetadata secondary, SearchKey key,
                                          boolean absentIsComplete) {
        BTreeIndex index = refreshRootSnapshot(secondary.index());
        MiniTransaction inverse = mtrMgr.begin(mtrMgr.budgetFor(RedoBudgetPurpose.ROLLBACK_INVERSE,
                BTreeRedoBudgetEstimator.structuralDelete(index.rootLevel())));
        try {
            BTreeSecondaryRemovalResult result = btree.deletePublishedSecondary(inverse, index, key);
            if (result.status() == SecondaryEntryRemovalStatus.STATE_CONFLICT
                    || (result.status() == SecondaryEntryRemovalStatus.ABSENT && !absentIsComplete)) {
                throw new UndoLogFormatException("rollback secondary remove state mismatch: index="
                        + index.indexId() + " key=" + key + " status=" + result.status());
            }
            mtrMgr.commit(inverse);
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(inverse, error);
            throw error;
        }
    }

    /** revive 旧二级 identity；缺失不是幂等完成态，因为目标旧 entry 在 purge barrier 下必须仍可恢复。 */
    private void reviveSecondary(SecondaryIndexMetadata secondary, SearchKey key) {
        applySecondaryDeleteState(secondary, key, false, false);
    }

    /** 把 UPDATE 前被 revive 的新 identity 恢复为 delete-marked；缺失表示 undo 前态无法再重建。 */
    private void markSecondaryDeleted(SecondaryIndexMetadata secondary, SearchKey key) {
        applySecondaryDeleteState(secondary, key, true, false);
    }

    /** 在独立 point-rewrite MTR 中翻转二级 delete 位，并按调用方语义分类 ABSENT。
     *
     * @param secondary 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param key 参与 {@code applySecondaryDeleteState} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param deleted 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
     * @param absentIsComplete 恢复容错策略标志；只允许在契约明确的损坏或结果不确定场景放宽校验，不得掩盖其他数据损坏
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void applySecondaryDeleteState(SecondaryIndexMetadata secondary, SearchKey key,
                                           boolean deleted, boolean absentIsComplete) {
        BTreeIndex index = refreshRootSnapshot(secondary.index());
        MiniTransaction inverse = mtrMgr.begin(mtrMgr.budgetFor(RedoBudgetPurpose.ROLLBACK_INVERSE,
                BTreeRedoBudgetEstimator.pointRewrite()));
        try {
            BTreeSecondaryDeleteMarkResult result = btree.setSecondaryDeleteMark(inverse, index, key, deleted);
            if (result.status() == SecondaryDeleteMarkStatus.ABSENT && !absentIsComplete) {
                throw new UndoLogFormatException("rollback secondary mark state mismatch: index="
                        + index.indexId() + " key=" + key + " targetDeleted=" + deleted);
            }
            mtrMgr.commit(inverse);
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(inverse, error);
            throw error;
        }
    }

    /** 使用独立只读 MTR 刷新 root level，提交释放 root S latch 后才允许开启二级写 MTR。 */
    private BTreeIndex refreshRootSnapshot(BTreeIndex index) {
        MiniTransaction read = mtrMgr.beginReadOnly();
        try {
            BTreeIndex refreshed = rootSnapshots.refresh(read, index);
            mtrMgr.commit(read);
            return refreshed;
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(read, error);
            throw error;
        }
    }

    /** secondary tail 只能由完整表级生产 wiring 消费，legacy 单索引实例不得静默忽略二级恢复证据。 */
    private void requireMultiIndexRollbackWiring() {
        if (targetResolver == null || rootSnapshots == null || rowGuards == null) {
            throw new DatabaseValidationException(
                    "secondary rollback requires target resolver, root snapshots and shared row guards");
        }
    }

    /** 构造包含 undo 与 mutation identity 的 fail-closed 二级恢复异常。 */
    private static UndoLogFormatException secondaryInverseMismatch(UndoRecord record,
                                                                   SecondaryUndoMutation mutation,
                                                                   String detail) {
        return new UndoLogFormatException("secondary inverse mismatch: table=" + record.tableId()
                + " clusteredIndex=" + record.indexId() + " secondaryIndex=" + mutation.indexId()
                + " action=" + mutation.action() + ": " + detail);
    }

    /**
     * 在所有 statement 逆操作成功后，用独立短写 MTR compare-and-set first-page 持久 logical head。marker
     * commit 返回前绝不移动内存 context；stale expected、目标 record 损坏或 MTR 失败都会向上传播，使 Guard
     * 把事务标成 rollback-only。由于 marker 永不领先数据修改，crash 最坏只会让 recovery 幂等重做已完成逆操作。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param expectedHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param targetHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param current 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     */
    private void persistLogicalHead(PageId firstPageId, UndoLogicalHead expectedHead,
                                    UndoLogicalHead targetHead, RecordAt current,
                                    BTreeIndex clusteredIndex) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        BTreeIndex markerIndex = targetHead.isEmpty()
                ? current.index()
                : resolveIndexAt(firstPageId, targetHead.rollPointer(), clusteredIndex);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        List<LobFreeBatchPlan> lobFreePlans = planRollbackLobFrees(List.of(current));
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        RedoBudgetWorkload markerWorkload = rollbackMarkerWorkload(lobFreePlans);
        MiniTransaction markerMtr = mtrMgr.begin(
                mtrMgr.budgetFor(RedoBudgetPurpose.ROLLBACK_MARKER, markerWorkload));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            UndoLogSegment writable = undoAccess.open(
                    markerMtr, firstPageId, PageLatchMode.EXCLUSIVE);
            try (var ignored = markerMtr.allowOutOfOrderPageLatch(
                    "rollback marker owns undo first-page before LOB/FSP; LOB/FSP never waits for undo pages")) {
                freeRollbackLobs(markerMtr, lobFreePlans);
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
     * 恢复隔离记录的专用 marker MTR：只验证 expected/target record 并更新系统 undo first-page，
     * 不规划或释放 LOB，也不访问任何用户 B+Tree 页。
     */
    private void persistRecoveryUnavailableLogicalHead(PageId firstPageId,
                                                       UndoLogicalHead expectedHead,
                                                       UndoLogicalHead targetHead,
                                                       RecordAt current,
                                                       BTreeIndex clusteredIndex) {
        BTreeIndex markerIndex = targetHead.isEmpty()
                ? current.index()
                : resolveIndexAt(firstPageId, targetHead.rollPointer(), clusteredIndex);
        MiniTransaction marker = mtrMgr.begin(mtrMgr.budgetFor(
                RedoBudgetPurpose.ROLLBACK_MARKER, rollbackMarkerWorkload(List.of())));
        try {
            UndoLogSegment writable = undoAccess.open(marker, firstPageId, PageLatchMode.EXCLUSIVE);
            writable.updateLogicalHead(expectedHead, targetHead,
                    markerIndex.keyDef(), markerIndex.schema());
            mtrMgr.commit(marker);
        } catch (RuntimeException error) {
            rollbackActiveStateMtr(marker, error);
            throw error;
        }
    }

    /**
     * partial rollback 的所有 inverse 完成后，在一个 marker MTR 中按 PageId 排序更新一个或两个 first-page heads。
     * marker 永不领先数据修改；任一 CAS 失败都不发布内存保存点边界，事务由上层转为 rollback-only。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param updates 参与 {@code persistLogicalHeads} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param appliedRecords 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     */
    private void persistLogicalHeads(List<HeadUpdate> updates, BTreeIndex clusteredIndex,
                                     List<RecordAt> appliedRecords) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (updates.isEmpty()) {
            return;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        List<ResolvedHeadUpdate> ordered = updates.stream()
                .map(update -> new ResolvedHeadUpdate(update,
                        update.target().isEmpty() ? requireFallbackIndex(clusteredIndex)
                                : resolveIndexAt(update.firstPageId(), update.target().rollPointer(), clusteredIndex)))
                .sorted(Comparator.comparingInt((ResolvedHeadUpdate item) -> item.firstPageId().spaceId().value())
                        .thenComparingLong(item -> item.firstPageId().pageNo().value()))
                .toList();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        List<LobFreeBatchPlan> lobFreePlans = planRollbackLobFrees(appliedRecords);
        MiniTransaction marker = mtrMgr.begin(mtrMgr.budgetFor(
                RedoBudgetPurpose.ROLLBACK_MARKER, rollbackMarkerWorkload(lobFreePlans)));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            try (var ignored = marker.allowOutOfOrderPageLatch(
                    "dual undo marker: target records may live on non-monotonic chain pages after sorted first pages")) {
                // 先固定全部 first-page X guard，随后才触碰 LOB/FSP；logical head 最后写，不能领先 ownership free。
                List<UndoLogSegment> writableSegments = new ArrayList<>(ordered.size());
                for (ResolvedHeadUpdate resolved : ordered) {
                    writableSegments.add(undoAccess.open(marker, resolved.firstPageId(), PageLatchMode.EXCLUSIVE));
                }
                freeRollbackLobs(marker, lobFreePlans);
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

    /**
     * 合并 marker 固定 first-page/header 余量与全部 authoritative LOB 批量 free workload。
     *
     * @param plans 已按 segment 稳定排序且完成 reference/segment 基本校验的释放计划。
     * @return 覆盖 logical-head CAS 和所有 FSP free/PAGE_INIT 的动态 redo workload。
     * @throws DatabaseValidationException 计划容器缺失或包含 {@code null} 时抛出。
     */
    private RedoBudgetWorkload rollbackMarkerWorkload(List<LobFreeBatchPlan> plans) {
        if (plans == null || plans.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("rollback marker LOB free plans must not contain null");
        }
        RedoBudgetWorkload workload = RedoBudgetWorkload.pageImages(6L);
        for (LobFreeBatchPlan plan : plans) workload = workload.plus(plan.workload());
        return workload;
    }

    /**
     * 从已完成 inverse 的 undo records 投影 rollback-owned LOB，并按 authoritative segment 聚合为批量计划。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>逐 record 校验 exact schema ordinal 和 LOB storage wiring。</li>
     *     <li>INSERT 收集 inserted ownership；UPDATE 只收集 LV 中的 rollback-new ownership。</li>
     *     <li>按 authoritative segment 分组并调用 LobStorage 拒绝重复 physical reference。</li>
     *     <li>按 space/inode/segment identity 稳定排序，供 marker 维持确定 FSP 访问顺序。</li>
     * </ol>
     *
     * @param records 当前 marker 即将越过的 undo records；不能为空容器或包含 {@code null}。
     * @return 按 authoritative segment 排序的不可变批量 free plans；无 ownership 时为空列表。
     * @throws UndoLogFormatException metadata/ordinal/segment 缺失或 rollback 未配置 LobStorage 时抛出。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private List<LobFreeBatchPlan> planRollbackLobFrees(List<RecordAt> records) {
        if (records == null || records.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("rollback marker records must not contain null");
        }
        // 1. 分组前先验证每条 record 的 exact schema/target；legacy 无 LOB 路径保持空结果。
        Map<SegmentRef, List<LobFreeTarget>> targetsBySegment = new LinkedHashMap<>();
        for (RecordAt at : records) {
            List<LobFreeTarget> targets = new ArrayList<>();
            for (var ownership : at.record().insertedLobs()) {
                targets.add(rollbackFreeTarget(at, ownership.columnOrdinal(), ownership.value(), "INSERT"));
            }
            // 2. purge-old ownership 在 rollback 必须保留；这里只消费前向 UPDATE 新链。
            for (var ownership : at.record().lobVersionOwnerships()) {
                ownership.rollbackNewValue().ifPresent(external -> targets.add(
                        rollbackFreeTarget(at, ownership.columnOrdinal(), external, "UPDATE")));
            }
            if (targets.isEmpty()) continue;
            if (lobStorage == null) {
                throw new UndoLogFormatException("LOB rollback storage is not configured");
            }
            SegmentRef segment = at.target().lobSegment().orElseThrow(() -> new UndoLogFormatException(
                    "undo target lacks authoritative LOB segment"));
            targetsBySegment.computeIfAbsent(segment, ignored -> new ArrayList<>()).addAll(targets);
        }
        // 3. LobStorage 统一校验 reference→segment identity 和批内重复 ownership。
        List<LobFreeBatchPlan> plans = new ArrayList<>(targetsBySegment.size());
        targetsBySegment.forEach((segment, targets) -> plans.add(lobStorage.planFreeBatch(segment, targets)));
        // 4. 多表/多 segment marker 采用稳定物理顺序，避免 FSP latch 获取顺序随 Hash/undo 组合漂移。
        plans.sort(Comparator.comparingInt((LobFreeBatchPlan plan) -> plan.segment().spaceId().value())
                .thenComparingInt(plan -> plan.segment().inodeSlot())
                .thenComparingLong(plan -> plan.segment().segmentId().value()));
        return List.copyOf(plans);
    }

    /**
     * 将单条 undo ownership 转换为带 exact schema 类型的 LOB free target。
     *
     * @param at       已解析 exact-version target 的 undo record。
     * @param ordinal  ownership 声明的 schema ordinal。
     * @param external 待 rollback 释放的新 external envelope。
     * @param kind     INSERT/UPDATE 诊断标签。
     * @return 交给 LobStorage 批量校验/释放的不可变 target。
     * @throws UndoLogFormatException ordinal 越界或 authoritative LOB segment 缺失时抛出。
     */
    private LobFreeTarget rollbackFreeTarget(RecordAt at, int ordinal,
                                             cn.zhangyis.db.storage.record.type.ColumnValue.ExternalValue external,
                                             String kind) {
        if (ordinal < 0 || ordinal >= at.index().schema().columns().size()) {
            throw new UndoLogFormatException(kind + " LOB ownership column ordinal out of schema: " + ordinal);
        }
        if (at.target().lobSegment().isEmpty()) {
            throw new UndoLogFormatException(kind + " LOB ownership requires authoritative DD LOB segment");
        }
        return new LobFreeTarget(ordinal, at.index().schema().column(ordinal).type(), external);
    }

    /**
     * 在已固定 undo first-page X latch 的 marker MTR 中执行全部批量 free；调用方随后才允许写 logical head。
     *
     * @param marker ACTIVE marker MTR，redo budget 已覆盖全部计划。
     * @param plans  按 authoritative segment 稳定排序的批量 free plans。
     * @throws DatabaseRuntimeException 链校验、FSP free 或 PAGE_INIT 失败时抛出；调用方必须保留 fail-stop 严重度。
     */
    private void freeRollbackLobs(MiniTransaction marker, List<LobFreeBatchPlan> plans) {
        for (LobFreeBatchPlan plan : plans) lobStorage.freePlannedBatch(marker, plan);
    }

    /**
     * 写回滚完成正式恢复 redo。它在 slot release / {@code finishRollback} 之前执行，此时事务仍处于
     * ROLLING_BACK，redo record 能记录真实的 from-state；若写入失败，slot 仍由本事务占用，调用方可安全重试。
     * recovery table 消费该 record 重建终态和 counter 高水位；若事务存在 undo slot，恢复仍必须与 page3/header
     * 交叉校验，不能把 logical redo 当成绕过物理状态检查的许可。
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
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

    /**
     * 校验当前状态后推进事务、MVCC 与锁状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param stateMtr 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /** 同一 recovered ACTIVE 事务的一个或两个局部链按全局 undoNo 归并回滚，并一次性终结全部 slots。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param logs 参与 {@code rollbackRecovered} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param creatorTrxId 参与 {@code rollbackRecovered} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code rollbackRecovered} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSummary rollbackRecovered(Collection<RecoveredUndoLogIdentity> logs,
                                              TransactionId creatorTrxId,
                                              BTreeIndex clusteredIndex) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (logs == null || logs.isEmpty() || creatorTrxId == null
                || clusteredIndex == null && indexResolver == null && targetResolver == null) {
            throw new DatabaseValidationException("recovered rollback group fields must not be empty/null");
        }
        List<UndoLogBinding> bindings = new ArrayList<>();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        RollbackSummary progress = rollbackRecoveredBindings(bindings, clusteredIndex);
        finalizer.finalizeRecoveredRollback(bindings, creatorTrxId);
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return progress;
    }

    /** recovered ACTIVE/PREPARED 共用逐记录循环；隔离记录只推进 head，健康记录仍执行完整 inverse。 */
    private RollbackSummary rollbackRecoveredBindings(Collection<UndoLogBinding> source,
                                                       BTreeIndex clusteredIndex) {
        List<UndoLogBinding> bindings = new ArrayList<>(source);
        int applied = 0;
        int skipped = 0;
        while (true) {
            UndoLogBinding binding = newestRecoveredBinding(bindings);
            if (binding == null) {
                break;
            }
            UndoLogicalHead expectedHead = binding.logicalHead();
            RecordAt current = readUndoRecord(binding.firstPageId(), clusteredIndex,
                    expectedHead.rollPointer());
            UndoLogicalHead targetHead = derivePredecessorHead(
                    binding.firstPageId(), clusteredIndex, expectedHead, current);
            if (current.target().recoveryUnavailable()) {
                persistRecoveryUnavailableLogicalHead(binding.firstPageId(), expectedHead,
                        targetHead, current, clusteredIndex);
                skipped++;
            } else {
                applyUndoRecordInOwnMtr(current, clusteredIndex);
                applied++;
                progressFaultInjector.after(RollbackProgressPhase.AFTER_INVERSE_COMMIT, expectedHead);
                persistLogicalHead(binding.firstPageId(), expectedHead, targetHead, current,
                        clusteredIndex);
            }
            binding.publishHead(targetHead);
            progressFaultInjector.after(RollbackProgressPhase.AFTER_PROGRESS_COMMIT, targetHead);
        }
        return new RollbackSummary(applied, skipped);
    }

    /** recovery 生产入口；每条 undo identity 通过 DD target resolver 定位，不接受 last-index fallback。
     *
     * @param logs 参与 {@code rollbackRecovered} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param creatorTrxId 参与 {@code rollbackRecovered} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code rollbackRecovered} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSummary rollbackRecovered(Collection<RecoveredUndoLogIdentity> logs,
                                              TransactionId creatorTrxId) {
        if (targetResolver == null) {
            throw new DatabaseValidationException("resolved recovered rollback requires target resolver");
        }
        return rollbackRecovered(logs, creatorTrxId, null);
    }

    /**
     * 执行事务、MVCC 与锁恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param binding 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** live/statement/savepoint 前检必须在事务状态转换和用户页修改前拒绝隔离目标。 */
    private static void requireLiveTargetAvailable(RecordAt at) {
        if (at.target().recoveryUnavailable()) {
            throw new UndoTargetUnavailableException(at.record().tableId());
        }
    }

    /**
     * 执行事务、MVCC 与锁恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * @param bindings 参与 {@code newestRecoveredBinding} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code newestRecoveredBinding} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /**
     * 执行事务、MVCC 与锁恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code readRecoveredKind} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
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

    /** 用独立只读 MTR 读取持久权威 logical head，供 live preflight 与 recovery 建链，返回前释放 page 资源。
     *
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code readPersistentLogicalHead} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
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
                ? legacyTarget(tableId, resolveIndex(tableId, indexId, fallback))
                : targetResolver.resolveTarget(tableId, indexId);
        BTreeIndex index = target.clusteredIndex();
        if (index.indexId() != indexId || !index.clustered()) {
            throw new UndoLogFormatException("undo identity resolved wrong/non-clustered target: table="
                    + tableId + " index=" + indexId + " resolved=" + index.indexId());
        }
        return target;
    }

    /** legacy 单聚簇入口只为现有低层测试/显式 API 构造无二级、无 LOB 的表级包装。 */
    private static UndoTargetMetadata legacyTarget(long tableId, BTreeIndex clusteredIndex) {
        return new UndoTargetMetadata(new TableIndexMetadata(tableId, clusteredIndex.schema().schemaVersion(),
                clusteredIndex, List.of()), java.util.Optional.empty());
    }

    private static BTreeIndex requireFallbackIndex(BTreeIndex fallback) {
        if (fallback == null) {
            throw new DatabaseValidationException("rollback requires an index resolver or fallback index");
        }
        return fallback;
    }

    /**
     * 封装事务、MVCC 与锁中 {@code RecordAt} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param pointer 参与 {@code 构造} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param target 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     */
    private record RecordAt(UndoRecord record, RollPointer pointer, UndoTargetMetadata target) {
        private BTreeIndex index() {
            return target.clusteredIndex();
        }
    }

    /**
     * 封装事务、MVCC 与锁中 {@code HeadUpdate} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param expected 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param target 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     */
    private record HeadUpdate(PageId firstPageId, UndoLogicalHead expected, UndoLogicalHead target) {
    }

    /**
     * 封装事务、MVCC 与锁中 {@code ResolvedHeadUpdate} 已校验但尚待发布的事务阶段状态；字段共同固定 owner、物理证据和补偿边界，防止提交/回滚重复执行。
     *
     * @param update 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     */
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param rec 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param rp 参与 {@code applyUndoRecord} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
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

    /** INSERT inverse 可能沿整棵树 merge/shrink；UPDATE/DELETE_MARK inverse 仅做单叶页等长改写。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code rollbackInverseWorkload} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
