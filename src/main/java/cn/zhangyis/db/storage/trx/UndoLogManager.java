package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.InsertedLobOwnership;
import cn.zhangyis.db.storage.undo.LobVersionOwnership;
import cn.zhangyis.db.storage.undo.SecondaryUndoMutation;
import cn.zhangyis.db.storage.undo.RollbackSegmentFreeListBase;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;
import cn.zhangyis.db.storage.undo.UndoSpaceReservation;

import java.util.List;
import java.util.Optional;
import java.util.ArrayList;

/**
 * 事务 undo 门面（设计 §5.2/§7.1/§7.2）。在 {@code storage.trx} 持事务语义，调用
 * {@code storage.undo} 物理设施（{@link UndoLogSegmentAccess}）写 INSERT/UPDATE/DELETE_MARK undo record，
 * 返回真 {@link RollPointer} 供聚簇记录盖 {@code DB_ROLL_PTR}，并在 commit/rollback/recovery/purge
 * 流程中维护 undo slot 与 history list。
 *
 * <p><b>依赖方向</b>：{@code storage.trx → storage.undo}。本类 import {@link MiniTransaction}（物理短事务）、
 * {@link UndoLogSegmentAccess}/{@link UndoRecord}（undo 物理设施）与 record schema/type；不反向暴露
 * {@link Transaction} 内部给 undo。{@code storage.undo} 不 import 本类或 {@code Transaction}。
 *
 * <p><b>当前范围</b>：生产 DML 与测试协作者统一通过
 * {@code planInsert/planUpdate/planDelete + appendPlanned} 在业务 MTR admission 前冻结目标 kind、持久头快照、
 * inline/external 编码、精确 reservation 与 redo workload。INSERT/UPDATE 首写分别惰性创建独立 slot/segment；
 * XA phase one/phase two 复用相同普通 undo owner；多 rseg 调度仍留后续切片。
 *
 * <p><b>WAL/失败边界</b>（§7.2）：生产 append 与聚簇修改仍在同一 MTR，undo root/payload redo 与 index redo
 * 同批提交。stale/编码/配置错误在 reservation 前拒绝；进入 segment/payload 物理修改后，或 undo 已写而聚簇修改
 * 失败，统一抛 {@link UndoWriteFatalException}，因为 MTR rollback 不撤销 buffer content，调用方不得在同进程重试。
 *
 * <p><b>并发</b>：slot 认领由 {@link RollbackSegmentSlotManager} 的 {@code ReentrantLock} 串行，锁内不分配页、
 * 不访问 BufferPool、不等待 IO；页分配（{@link UndoLogSegmentAccess#create}）在 slot 锁外完成。本片单 writer
 * 假设：同一事务/undo segment 同时只有一个 EXCLUSIVE append 会话。
 */
public final class UndoLogManager {

    /** undo 物理设施入口；生产 plan/append 均经它 create/open/resolve undo segment。 */
    private final UndoLogSegmentAccess access;
    /** 内存 rseg slot 目录；每个 kind 首写时各认领一个 slot。固定单一默认 rseg。 */
    private final RollbackSegmentSlotManager slotManager;
    /** undo 表空间；目标 kind 尚无 binding 时由 planned append 在此空间惰性建段。 */
    private final SpaceId undoSpace;
    /** 已提交 update/delete undo 的 history list；纯 insert undo 在 commit finalization 中按条件缓存或回收。 */
    private final HistoryList history;
    /**
     * 持久 rseg header 仓储。首写 claim 与 undo segment 创建同一 MTR；终态 clear 统一交给 finalizer。
     */
    private final RollbackSegmentHeaderRepository headerRepo;
    /** commit/rollback/purge 共享的 atomic cache/free/drop + page3 owner 转移协作者。 */
    private final UndoSegmentFinalizer finalizer;
    /** page3 cache/free owner 的统一运行期投影。 */
    private final UndoSegmentReuseDirectory reuseDirectory;

    /**
     * 构造生产 undo 门面。slot claim 必须持久到 page3；纯 insert commit 必须经 finalizer 在同一 redo batch
     * cache/free/drop segment + 转移 page3 owner，故不再提供绕过持久生命周期的构造方式。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param access      undo 物理设施入口，不能为 null。
     * @param slotManager 内存 rseg slot 目录，不能为 null。
     * @param undoSpace   undo 表空间，不能为 null。
     * @param history     已提交 undo log 的 history list，不能为 null。
     * @param headerRepo  持久 rseg header 仓储。
     * @param finalizer   atomic undo segment 终态协作者。
     * @param reuseDirectory 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogManager(UndoLogSegmentAccess access, RollbackSegmentSlotManager slotManager, SpaceId undoSpace,
                           HistoryList history, RollbackSegmentHeaderRepository headerRepo,
                           UndoSegmentFinalizer finalizer, UndoSegmentReuseDirectory reuseDirectory) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (access == null || slotManager == null || undoSpace == null || history == null
                || headerRepo == null || finalizer == null || reuseDirectory == null) {
            throw new DatabaseValidationException("undo log manager args must not be null");
        }
        this.access = access;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.slotManager = slotManager;
        this.undoSpace = undoSpace;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.history = history;
        this.headerRepo = headerRepo;
        this.finalizer = finalizer;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.reuseDirectory = reuseDirectory;
    }

    /**
     * 在写 MTR admission 前规划不带二级 tail 的兼容 INSERT undo。
     *
     * @param txn        当前 ACTIVE 写事务；事务上下文决定 undo kind、slot 与 next undo number。
     * @param tableId    rollback/recovery 定位 exact table metadata 的稳定表 id。
     * @param indexId    聚簇索引稳定 id，undo 主体始终以它定位版本链 anchor。
     * @param clusterKey 按聚簇 key definition 顺序物化的完整主键值。
     * @param keyDef     解码聚簇主键各 part 类型的 exact-version key definition。
     * @param schema     编码 key 与后续旧 image 的 exact-version 聚簇表 schema。
     * @return 已冻结 encoded length、页 reservation 与 redo workload 的写计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 INSERT undo 时抛出。
     */
    public UndoWritePlan planInsert(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, IndexKeyDef keyDef, TableSchema schema) {
        return planInsert(txn, tableId, indexId, clusterKey, List.of(), keyDef, schema);
    }

    /**
     * 在写 MTR admission 前规划带二级发布证据的 INSERT undo。mutation 列表已经按 index id 排序并冻结，
     * codec 长度会进入 external payload 与 redo budget 计算，不能在 begin MTR 后追加。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在访问事务 undo context 前校验主键、secondary mutation、key definition 与 schema。</li>
     *     <li>解析/创建 INSERT undo planning context，冻结 next undo number、事务 id 与逻辑链前驱。</li>
     *     <li>构造含 secondary tail 的不可变 UndoRecord，并据编码长度冻结页 reservation 与 redo workload。</li>
     * </ol>
     *
     * @param txn                当前 ACTIVE 写事务。
     * @param tableId            rollback/recovery 定位 exact table metadata 的稳定表 id。
     * @param indexId            聚簇索引稳定 id。
     * @param clusterKey         按聚簇 key definition 顺序物化的完整主键值。
     * @param secondaryMutations 按 index id 严格递增的 INSERT_ENTRY 反向证据；构造 record 时防御性复制。
     * @param keyDef             exact-version 聚簇主键定义。
     * @param schema             exact-version 完整聚簇表 schema。
     * @return 编码形状、页 reservation 与 redo workload 均已冻结的 INSERT undo 写计划。
     * @throws TransactionStateException 参数缺失或事务状态/undo kind 不允许规划时抛出。
     * @throws DatabaseValidationException mutation 排序、action 或 record identity 不满足可恢复约束时抛出。
     */
    public UndoWritePlan planInsert(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    IndexKeyDef keyDef, TableSchema schema) {
        // 1. 纯输入校验先于事务 context 访问，失败不会分配 undo slot 或推进 undo number。
        if (clusterKey == null || secondaryMutations == null || keyDef == null || schema == null) {
            throw new TransactionStateException(
                    "planInsert clusterKey/secondaryMutations/keyDef/schema must not be null");
        }
        // 2. planning context 固定本次 INSERT log owner、next undo number 和局部链前驱。
        PlanningContext context = planningContext(txn, UndoLogKind.INSERT);

        // 3. secondary tail 进入实际 codec 长度，buildPlan 后调用方不得再修改 workload。
        UndoRecord record = UndoRecord.insert(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, List.of(), secondaryMutations, context.logicalHead().rollPointer());
        return buildPlan(context, record, keyDef, schema);
    }

    /**
     * 为 LOB-aware INSERT 冻结 deferred undo。placeholder ownership 必须与稍后实际 allocation 具有同一编码形状；
     * 它只参与 codec、页数和 admission 计算，绝不会由该接口直接写入普通 UNDO record slot。
     *
     * @param txn                   当前 ACTIVE 写事务。
     * @param tableId               undo 记录所属稳定表 id。
     * @param indexId               聚簇索引稳定 id。
     * @param clusterKey            完整物化聚簇主键值。
     * @param placeholderOwnerships LOB 规划产生的定形 ownership 占位列表。
     * @param keyDef                exact-version 聚簇主键定义。
     * @param schema                exact-version 完整聚簇表 schema。
     * @return secondary tail 为空、LOB 物理形状已冻结的 deferred INSERT undo 计划。
     * @throws TransactionStateException 参数缺失或事务状态不允许规划时抛出。
     */
    public DeferredInsertUndoPlan planDeferredInsert(Transaction txn, long tableId, long indexId,
                                                     List<ColumnValue> clusterKey,
                                                     List<InsertedLobOwnership> placeholderOwnerships,
                                                     IndexKeyDef keyDef, TableSchema schema) {
        return planDeferredInsert(txn, tableId, indexId, clusterKey, placeholderOwnerships,
                List.of(), keyDef, schema);
    }

    /**
     * 为表级 INSERT 同时冻结 LOB placeholder 与全部二级发布证据。actual LOB 只可替换定宽首页号，
     * secondary 列表在 deferred actual record 中保持逐项不变。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在访问事务 context 前校验主键、LOB placeholder、secondary mutation 与 exact-version schema/key。</li>
     *     <li>解析 INSERT planning context，并把 placeholder ownership 与 secondary tail 一起编码成物理形状基准。</li>
     *     <li>冻结普通 undo 写计划和 LOB shape validator；实际 LOB allocation 只能替换等长字段，不能改变页/redo 预算。</li>
     * </ol>
     *
     * @param txn                   当前 ACTIVE 写事务。
     * @param tableId               undo 记录所属稳定表 id。
     * @param indexId               聚簇索引稳定 id。
     * @param clusterKey            完整物化聚簇主键值。
     * @param placeholderOwnerships 按 column ordinal 递增的 LOB ownership 形状占位列表。
     * @param secondaryMutations    按 index id 递增的 INSERT_ENTRY 反向证据。
     * @param keyDef                exact-version 聚簇主键定义。
     * @param schema                exact-version 完整聚簇表 schema。
     * @return 同时冻结 LOB 与 secondary tail 编码形状的 deferred INSERT undo 计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 INSERT undo 时抛出。
     * @throws DatabaseValidationException ownership/mutation 排序或 action/type 组合无效时抛出。
     */
    public DeferredInsertUndoPlan planDeferredInsert(Transaction txn, long tableId, long indexId,
                                                     List<ColumnValue> clusterKey,
                                                     List<InsertedLobOwnership> placeholderOwnerships,
                                                     List<SecondaryUndoMutation> secondaryMutations,
                                                     IndexKeyDef keyDef, TableSchema schema) {
        // 1. 全部可变规划输入在访问事务 context 前检查；失败不改变 undo owner/slot 状态。
        if (clusterKey == null || placeholderOwnerships == null || secondaryMutations == null
                || keyDef == null || schema == null) {
            throw new TransactionStateException("deferred INSERT undo planning args must not be null");
        }
        // 2. placeholder 与 secondary tail 同时进入基准 record，确保编码顺序固定为 LOB -> secondary。
        PlanningContext context = planningContext(txn, UndoLogKind.INSERT);
        UndoRecord record = UndoRecord.insert(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, placeholderOwnerships, secondaryMutations, context.logicalHead().rollPointer());
        // 3. deferred plan 保存基准物理形状；实际 allocation 不能扩大 undo 页或 redo workload。
        return new DeferredInsertUndoPlan(buildPlan(context, record, keyDef, schema), keyDef, schema,
                placeholderOwnerships);
    }

    /**
     * 在同一业务 MTR 中先固定 undo owner/root/payload 页而不发布 placeholder。调用顺序必须位于 B+Tree prepare 与
     * LOB allocation 之前；返回 guard 后任何未发布退出均属于 fail-stop 边界。
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param deferred 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code prepareUndoAppend} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     */
    public PreparedUndoAppend<InsertedLobOwnership> prepareUndoAppend(Transaction txn, MiniTransaction mtr,
                                                                      DeferredInsertUndoPlan deferred) {
        return prepareDeferredUndoAppend(txn, mtr, deferred);
    }

    /**
     * 在同一 UPDATE 业务 MTR 中 prepare placeholder undo，但只允许稍后发布形状等价的真实 LV ownership。
     *
     * @param txn      当前 ACTIVE 写事务，必须与 deferred placeholder owner 一致。
     * @param mtr      已覆盖 undo+B+Tree+LOB workload 的 ACTIVE 业务 MTR。
     * @param deferred LOB replacement 规划阶段冻结的 deferred UPDATE undo。
     * @return 恰好一次接收真实 {@link LobVersionOwnership} 列表的 prepared append guard。
     * @throws TransactionStateException 参数缺失或事务不再 ACTIVE 时抛出。
     * @throws UndoWriteStalePlanException 事务 logical head、slot owner 或持久 append snapshot 已变化时抛出。
     * @throws UndoWriteFatalException prepare 已越过物理 owner/page 边界后失败时抛出。
     */
    public PreparedUndoAppend<LobVersionOwnership> prepareUndoAppend(Transaction txn, MiniTransaction mtr,
                                                                     DeferredUpdateUndoPlan deferred) {
        return prepareDeferredUndoAppend(txn, mtr, deferred);
    }

    /**
     * 根据冻结 acquisition 分派共享的 deferred owner/root/payload prepare 协议。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn      当前 ACTIVE 写事务。
     * @param mtr      已完成组合 redo admission 的业务 MTR。
     * @param deferred INSERT/UPDATE 对应的 placeholder 物理形状计划。
     * @param <T>      actual ownership 元素类型。
     * @return 与 acquisition 对应、恰好一次发布 actual record 的 prepared guard。
     * @throws TransactionStateException 参数缺失或事务状态无效时抛出。
     * @throws UndoWriteStalePlanException 事务 context 或持久 append snapshot 已变化时抛出。
     * @throws UndoWriteFatalException owner/page prepare 越过物理边界后失败时抛出。
     */
    private <T> PreparedUndoAppend<T> prepareDeferredUndoAppend(Transaction txn, MiniTransaction mtr,
                                                                DeferredUndoPlan<T> deferred) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null || mtr == null || deferred == null) {
            throw new TransactionStateException("prepareUndoAppend txn/mtr/plan must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        requireActiveTransaction(txn);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoWritePlan plan = deferred.placeholderPlan();
        if (!txn.transactionId().equals(plan.transactionId())) {
            throw new UndoWriteStalePlanException("deferred undo transaction id no longer matches target");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return switch (plan.acquisition()) {
            case ALLOCATE_NEW -> prepareAllocatedLog(txn, mtr, deferred);
            case REUSE_CACHED -> prepareCachedLog(txn, mtr, deferred);
            case REUSE_FREE -> prepareFreeLog(txn, mtr, deferred);
            case APPEND_EXISTING -> prepareExistingLog(txn, mtr, deferred);
        };
    }

    /**
     * 在写 MTR admission 前规划不带二级 tail 的兼容 UPDATE undo，并冻结完整旧行 image。
     *
     * @param txn              当前 ACTIVE 写事务。
     * @param tableId          undo 记录所属稳定表 id。
     * @param indexId          聚簇索引稳定 id。
     * @param clusterKey       被更新行的完整物化聚簇主键。
     * @param oldColumnValues  更新前按 schema 列序排列的全量用户列值。
     * @param oldHiddenColumns 更新前 DB_TRX_ID/DB_ROLL_PTR，是版本链和 CAS 前态。
     * @param keyDef           exact-version 聚簇主键定义。
     * @param schema           exact-version 完整聚簇表 schema。
     * @return secondary tail 为空的 UPDATE undo 写计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 UPDATE undo 时抛出。
     */
    public UndoWritePlan planUpdate(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, IndexKeyDef keyDef, TableSchema schema) {
        return planUpdate(txn, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns,
                List.of(), List.of(), keyDef, schema);
    }

    /**
     * 在写 MTR admission 前规划带二级 key-change 反向证据的 UPDATE undo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验完整主键、旧全列 image、旧隐藏列、secondary mutation 与 exact-version schema/key。</li>
     *     <li>解析 UPDATE undo planning context，冻结 next undo number、事务 owner 和局部链前驱。</li>
     *     <li>把旧版本与 CHANGE_KEY tail 编码为不可变 record，并冻结页 reservation/redo workload。</li>
     * </ol>
     *
     * @param txn                当前 ACTIVE 写事务。
     * @param tableId            undo 记录所属稳定表 id。
     * @param indexId            聚簇索引稳定 id。
     * @param clusterKey         被更新行的完整物化聚簇主键。
     * @param oldColumnValues    更新前按 schema 列序排列的全量用户列值。
     * @param oldHiddenColumns   更新前隐藏列，是版本链前驱和聚簇 CAS 证据。
     * @param secondaryMutations 按 index id 递增的 CHANGE_KEY 反向证据。
     * @param keyDef             exact-version 聚簇主键定义。
     * @param schema             exact-version 完整聚簇表 schema。
     * @return 编码形状、页 reservation 与 redo workload 已冻结的 UPDATE undo 写计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 UPDATE undo 时抛出。
     * @throws DatabaseValidationException mutation action/排序或旧 image 形状无效时抛出。
     */
    public UndoWritePlan planUpdate(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    IndexKeyDef keyDef, TableSchema schema) {
        return planUpdate(txn, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns,
                List.of(), secondaryMutations, keyDef, schema);
    }

    /**
     * 在写 MTR admission 前规划携带完整 LOB version ownership 与 secondary 证据的 UPDATE undo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验旧 image、LOB ownership、secondary mutation 和 exact codec metadata，失败不推进 undo number。</li>
     *     <li>读取 UPDATE-kind planning context，冻结 next undoNo、owner 和 logical predecessor。</li>
     *     <li>把 old image、LV 与 SI tail 同时编码，冻结 root/payload 页数和 redo workload。</li>
     * </ol>
     *
     * @param txn                  当前 ACTIVE 写事务。
     * @param tableId              undo target 稳定表 id。
     * @param indexId              聚簇索引稳定 id。
     * @param clusterKey           被更新行的完整聚簇主键。
     * @param oldColumnValues      更新前全量用户列。
     * @param oldHiddenColumns     更新前 DB_TRX_ID/DB_ROLL_PTR。
     * @param lobVersionOwnerships 按 ordinal 递增的旧链 purge/new 链 rollback ownership。
     * @param secondaryMutations   按 index id 递增的 CHANGE_KEY 证据。
     * @param keyDef               exact-version 聚簇 key definition。
     * @param schema               exact-version 完整表 schema。
     * @return 编码形状、页 reservation 和 redo workload 已冻结的 UPDATE undo 写计划。
     * @throws TransactionStateException 参数缺失、事务非 ACTIVE 或 planning context 不可用时抛出。
     * @throws DatabaseValidationException old image、LV/SI action 或排序不满足恢复不变量时抛出。
     */
    public UndoWritePlan planUpdate(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns,
                                    List<LobVersionOwnership> lobVersionOwnerships,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    IndexKeyDef keyDef, TableSchema schema) {
        // 1. 输入校验先于事务 context 访问，失败不会推进 undo number。
        if (clusterKey == null || oldColumnValues == null || oldHiddenColumns == null
                || lobVersionOwnerships == null || secondaryMutations == null
                || keyDef == null || schema == null) {
            throw new TransactionStateException("planUpdate old image/key/schema args must not be null");
        }
        // 2. UPDATE/DELETE 共用 UPDATE log kind，但 record type 在下一阶段固定为 UPDATE_ROW。
        PlanningContext context = planningContext(txn, UndoLogKind.UPDATE);

        // 3. 全量旧 image 与二级 mutation 同时进入 codec workload，begin MTR 后不可追加。
        UndoRecord record = UndoRecord.update(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, oldColumnValues, oldHiddenColumns, lobVersionOwnerships, secondaryMutations,
                context.logicalHead().rollPointer());
        return buildPlan(context, record, keyDef, schema);
    }

    /**
     * 为包含新 external allocation 的 UPDATE 冻结 placeholder LV tail。actual ownership 只允许替换新链首页号，
     * old image、purge flag、secondary tail 和物理编码长度均不能变化。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 old image、placeholder LV、secondary tail 和 exact codec metadata。</li>
     *     <li>读取 UPDATE-kind planning context，冻结 owner、acquisition、next undoNo 和 predecessor。</li>
     *     <li>编码 placeholder record 并冻结 root/payload 页数、redo workload 和 actual shape validator。</li>
     * </ol>
     *
     * @param txn                   当前 ACTIVE 写事务。
     * @param tableId               undo target 稳定表 id。
     * @param indexId               聚簇索引稳定 id。
     * @param clusterKey            被更新行的完整聚簇主键。
     * @param oldColumnValues       更新前全量用户列。
     * @param oldHiddenColumns      更新前隐藏列。
     * @param placeholderOwnerships 按 ordinal 递增的 LV placeholder ownership。
     * @param secondaryMutations    按 index id 递增的 CHANGE_KEY 证据。
     * @param keyDef                exact-version 聚簇 key definition。
     * @param schema                exact-version 完整表 schema。
     * @return actual 新 LOB 分配前可用于 admission/prepare 的 deferred UPDATE undo 计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 UPDATE-kind undo 时抛出。
     * @throws DatabaseValidationException placeholder LV/SI 或旧 image 无效时抛出。
     */
    public DeferredUpdateUndoPlan planDeferredUpdate(Transaction txn, long tableId, long indexId,
                                                       List<ColumnValue> clusterKey,
                                                       List<ColumnValue> oldColumnValues,
                                                       HiddenColumns oldHiddenColumns,
                                                       List<LobVersionOwnership> placeholderOwnerships,
                                                       List<SecondaryUndoMutation> secondaryMutations,
                                                       IndexKeyDef keyDef, TableSchema schema) {
        // 1. 全部可变输入在访问事务 context 前校验，失败不推进 undoNo 或占用 slot。
        if (clusterKey == null || oldColumnValues == null || oldHiddenColumns == null
                || placeholderOwnerships == null || secondaryMutations == null
                || keyDef == null || schema == null) {
            throw new TransactionStateException("deferred UPDATE undo planning args must not be null");
        }
        // 2. UPDATE-kind context 冻结 owner、acquisition、global undoNo 与 logical predecessor。
        PlanningContext context = planningContext(txn, UndoLogKind.UPDATE);
        // 3. placeholder LV 与 SI 一起进入 codec；actual 只可替换定宽 firstPageNo。
        UndoRecord record = UndoRecord.update(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, oldColumnValues, oldHiddenColumns, placeholderOwnerships, secondaryMutations,
                context.logicalHead().rollPointer());
        return new DeferredUpdateUndoPlan(buildPlan(context, record, keyDef, schema), keyDef, schema,
                placeholderOwnerships);
    }

    /**
     * 在写 MTR admission 前规划不带二级 tail 的兼容 DELETE_MARK undo。
     *
     * @param txn              当前 ACTIVE 写事务。
     * @param tableId          undo 记录所属稳定表 id。
     * @param indexId          聚簇索引稳定 id。
     * @param clusterKey       被删除行的完整物化聚簇主键。
     * @param oldColumnValues  删除前存活版本的全量用户列值。
     * @param oldHiddenColumns 删除前 DB_TRX_ID/DB_ROLL_PTR。
     * @param keyDef           exact-version 聚簇主键定义。
     * @param schema           exact-version 完整聚簇表 schema。
     * @return secondary tail 为空的 DELETE_MARK undo 写计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 UPDATE-kind undo 时抛出。
     */
    public UndoWritePlan planDelete(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, IndexKeyDef keyDef, TableSchema schema) {
        return planDelete(txn, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns,
                List.of(), List.of(), keyDef, schema);
    }

    /**
     * 在写 MTR admission 前规划带全部二级 delete-mark 反向证据的 DELETE undo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验完整主键、删除前全量 image/隐藏列、secondary mutation 与 exact-version schema/key。</li>
     *     <li>解析 UPDATE-kind planning context，冻结 next undo number、事务 owner 和局部链前驱。</li>
     *     <li>把存活旧版本与 DELETE_MARK_ENTRY tail 编码为不可变 record，并冻结页 reservation/redo workload。</li>
     * </ol>
     *
     * @param txn                当前 ACTIVE 写事务。
     * @param tableId            undo 记录所属稳定表 id。
     * @param indexId            聚簇索引稳定 id。
     * @param clusterKey         被删除行的完整物化聚簇主键。
     * @param oldColumnValues    删除前存活版本的全量用户列值。
     * @param oldHiddenColumns   删除前隐藏列，是 rollback revive 的版本链证据。
     * @param secondaryMutations 按 index id 递增的 DELETE_MARK_ENTRY 反向证据。
     * @param keyDef             exact-version 聚簇主键定义。
     * @param schema             exact-version 完整聚簇表 schema。
     * @return 编码形状、页 reservation 与 redo workload 已冻结的 DELETE_MARK undo 写计划。
     * @throws TransactionStateException 参数缺失或事务无法规划 UPDATE-kind undo 时抛出。
     * @throws DatabaseValidationException mutation action/排序或旧 image 形状无效时抛出。
     */
    public UndoWritePlan planDelete(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    IndexKeyDef keyDef, TableSchema schema) {
        return planDelete(txn, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns,
                List.of(), secondaryMutations, keyDef, schema);
    }

    /**
     * 规划携带旧 LOB purge ownership 的 DELETE_MARK undo；DELETE 不允许 rollback-new ownership。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验旧 image、LV/SI 列表和 exact codec metadata，失败不访问事务 undo context。</li>
     *     <li>读取 UPDATE-kind planning context，冻结 owner、next undoNo 和 logical predecessor。</li>
     *     <li>编码 DELETE old image、purge-only LV 和 secondary tail，冻结页数与 redo workload。</li>
     * </ol>
     *
     * @param txn                  当前 ACTIVE 写事务。
     * @param tableId              undo target 稳定表 id。
     * @param indexId              聚簇索引稳定 id。
     * @param clusterKey           被删除行的完整聚簇主键。
     * @param oldColumnValues      删除前存活版本全量用户列。
     * @param oldHiddenColumns     删除前隐藏列。
     * @param lobVersionOwnerships 按 ordinal 递增且只含 purge-old 动作的 ownership。
     * @param secondaryMutations   按 index id 递增的 DELETE_MARK_ENTRY 证据。
     * @param keyDef               exact-version 聚簇 key definition。
     * @param schema               exact-version 完整表 schema。
     * @return 编码形状、页 reservation 和 redo workload 已冻结的 DELETE undo 写计划。
     * @throws TransactionStateException 参数缺失或事务不能规划 UPDATE-kind undo 时抛出。
     * @throws DatabaseValidationException ownership/old image/secondary action 不满足恢复不变量时抛出。
     */
    public UndoWritePlan planDelete(Transaction txn, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns,
                                    List<LobVersionOwnership> lobVersionOwnerships,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    IndexKeyDef keyDef, TableSchema schema) {
        // 1. 输入校验先于事务 context 访问，失败不分配或推进 undo 状态。
        if (clusterKey == null || oldColumnValues == null || oldHiddenColumns == null
                || lobVersionOwnerships == null || secondaryMutations == null
                || keyDef == null || schema == null) {
            throw new TransactionStateException("planDelete old image/key/schema args must not be null");
        }
        // 2. DELETE_MARK 使用 UPDATE log kind，record type 在下一阶段明确区分恢复动作。
        PlanningContext context = planningContext(txn, UndoLogKind.UPDATE);

        // 3. 删除前存活版本与全部二级 revive 证据同时进入最终 codec workload。
        UndoRecord record = UndoRecord.deleteMark(context.nextUndoNo(), context.transactionId(), tableId, indexId,
                clusterKey, oldColumnValues, oldHiddenColumns, lobVersionOwnerships, secondaryMutations,
                context.logicalHead().rollPointer());
        return buildPlan(context, record, keyDef, schema);
    }

    /**
     * 在调用方已经按 {@link UndoWritePlan#redoWorkload()} 完成 admission 的 MTR 中执行计划。所有 stale 校验先于
     * reservation；进入 reservation/append 后任意失败转为 fatal，因为 MTR rollback 不撤销 buffer content。
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code appendPlanned} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public RollPointer appendPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        if (txn == null || mtr == null || plan == null) {
            throw new TransactionStateException("appendPlanned txn/mtr/plan must not be null");
        }
        requireActiveTransaction(txn);
        if (!txn.transactionId().equals(plan.transactionId())) {
            throw new UndoWriteStalePlanException("undo plan transaction id no longer matches target transaction");
        }
        return switch (plan.acquisition()) {
            case ALLOCATE_NEW -> appendAllocatedLogPlanned(txn, mtr, plan);
            case REUSE_CACHED -> appendCachedLogPlanned(txn, mtr, plan);
            case REUSE_FREE -> appendFreeLogPlanned(txn, mtr, plan);
            case APPEND_EXISTING -> appendExistingPlanned(txn, mtr, plan);
        };
    }

    private UndoWritePlan buildPlan(PlanningContext context, UndoRecord record,
                                    IndexKeyDef keyDef, TableSchema schema) {
        UndoRecordWritePlan physical = access.planRecord(record, keyDef, schema);
        int pages = switch (context.acquisition()) {
            case ALLOCATE_NEW -> access.plannedNewPages(true, null, physical);
            case REUSE_CACHED, REUSE_FREE -> physical.externalPageCount();
            case APPEND_EXISTING -> access.plannedNewPages(false, context.persistentSnapshot(), physical);
        };
        return new UndoWritePlan(context.transactionId(), context.kind(), context.acquisition(), context.firstPageId(),
                context.globalLastUndoNo(), context.logicalHead(), context.persistentSnapshot(),
                context.cachedCandidate(), context.freeCandidate(), physical, pages,
                UndoRedoBudgetEstimator.append(context.acquisition(), physical.externalPageCount()));
    }

    /** fresh INSERT log 的 owner/root prepare；segment 首页和 page3 slot 在 actual record 之前先成为稳定 owner。
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param deferred 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param <T> 调用方提供的类型参数，必须满足声明的上界约束
     * @return {@code prepareAllocatedLog} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     * @throws UndoWriteFatalException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private <T> PreparedUndoAppend<T> prepareAllocatedLog(Transaction txn, MiniTransaction mtr,
                                                          DeferredUndoPlan<T> deferred) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        UndoWritePlan plan = deferred.placeholderPlan();
        UndoContext current = txn.undoContext();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || current != null && current.hasBinding(plan.kind())) {
            throw new UndoWriteStalePlanException("new deferred undo log plan is stale");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        PreparedResources resources = new PreparedResources();
        boolean physical = false;
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            RollbackSegmentSlotManager.ClaimLease claim = resources.add(slotManager.reserveClaim());
            requirePersistentSlotFree(mtr, claim.slotId());
            UndoSpaceReservation reservation = resources.add(access.reservePages(mtr, undoSpace,
                    plan.pagesToReserve()));
            claim.physicalMutationStarted();
            physical = true;
            UndoLogSegment segment = access.create(mtr, undoSpace, plan.transactionId(), plan.kind());
            if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve() - 1) {
                throw new UndoWriteFatalException("new deferred undo segment page requirement differs from plan");
            }
            PageId firstPageId = segment.firstPageId();
            claim.bind(firstPageId);
            claimRsegSlotAfterUndoPage(mtr, claim.slotId(), firstPageId);
            UndoContext context = current == null ? new UndoContext(slotManager.rollbackSegmentId()) : current;
            context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId, UndoLogicalHead.EMPTY));
            segment.prepareAppend(plan.recordPlan());
            return preparedHandle(txn, mtr, deferred, plan, segment, context, resources);
        } catch (RuntimeException error) {
            resources.closeSuppressing(error);
            if (physical) {
                throw fatalAfterMutation("new deferred undo prepare failed after physical mutation", error);
            }
            throw error;
        }
    }

    /** cached owner transition 与首页激活均在 actual record 之前完成；失败保留 transition fence。 */
    private <T> PreparedUndoAppend<T> prepareCachedLog(Transaction txn, MiniTransaction mtr,
                                                       DeferredUndoPlan<T> deferred) {
        UndoWritePlan plan = deferred.placeholderPlan();
        UndoContext current = requireNoKindBinding(txn, plan, "cached deferred undo");
        PreparedResources resources = new PreparedResources();
        boolean physical = false;
        try {
            RollbackSegmentSlotManager.ClaimLease claim = resources.add(slotManager.reserveClaim());
            UndoSegmentReuseDirectory.CachePopLease cachePop = resources.add(
                    reuseDirectory.reserveCachePop(plan.cachedCandidate()));
            requirePersistentSlotFree(mtr, claim.slotId());
            if (plan.pagesToReserve() > 0) {
                resources.add(access.reservePages(mtr, undoSpace, plan.pagesToReserve()));
            }
            claim.physicalMutationStarted();
            cachePop.physicalMutationStarted();
            physical = true;
            var candidate = cachePop.candidate();
            headerRepo.moveCachedTopToActiveSlot(mtr, undoSpace, plan.kind(), candidate.expectedCount(),
                    candidate.segment().handle().firstPageId(), claim.slotId());
            UndoLogSegment segment = access.activateCached(mtr, candidate.segment(), plan.transactionId());
            if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve()) {
                throw new UndoWriteFatalException("cached deferred undo page requirement differs from plan");
            }
            PageId firstPageId = segment.firstPageId();
            claim.bind(firstPageId);
            cachePop.complete();
            UndoContext context = current == null ? new UndoContext(slotManager.rollbackSegmentId()) : current;
            context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId, UndoLogicalHead.EMPTY));
            segment.prepareAppend(plan.recordPlan());
            return preparedHandle(txn, mtr, deferred, plan, segment, context, resources);
        } catch (RuntimeException error) {
            resources.closeSuppressing(error);
            if (physical) {
                throw fatalAfterMutation("cached deferred undo prepare failed after owner transition", error);
            }
            throw error;
        }
    }

    /** free FIFO owner transition 的 deferred prepare，锁序与普通 free reuse 首写保持一致。
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param deferred 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param <T> 调用方提供的类型参数，必须满足声明的上界约束
     * @return {@code prepareFreeLog} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     * @throws UndoWriteFatalException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private <T> PreparedUndoAppend<T> prepareFreeLog(Transaction txn, MiniTransaction mtr,
                                                     DeferredUndoPlan<T> deferred) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        UndoWritePlan plan = deferred.placeholderPlan();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoContext current = requireNoKindBinding(txn, plan, "free deferred undo");
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        PreparedResources resources = new PreparedResources();
        boolean physical = false;
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            RollbackSegmentSlotManager.ClaimLease claim = resources.add(slotManager.reserveClaim());
            UndoSegmentReuseDirectory.FreePopLease freePop = resources.add(
                    reuseDirectory.reserveFreePop(plan.freeCandidate()));
            requirePersistentSlotFree(mtr, claim.slotId());
            if (plan.pagesToReserve() > 0) {
                resources.add(access.reservePages(mtr, undoSpace, plan.pagesToReserve()));
            }
            claim.physicalMutationStarted();
            freePop.physicalMutationStarted();
            physical = true;
            var candidate = freePop.candidate();
            RollbackSegmentFreeListBase expectedBase = new RollbackSegmentFreeListBase(
                    Optional.of(candidate.segment().handle().firstPageId()),
                    Optional.of(candidate.expectedTail().handle().firstPageId()), candidate.expectedCount());
            Optional<PageId> successor = candidate.successor().map(item -> item.handle().firstPageId());
            headerRepo.moveFreeHeadToActiveSlot(mtr, undoSpace, expectedBase,
                    candidate.segment().handle().firstPageId(), successor, claim.slotId());
            UndoLogSegment segment = access.activateFree(mtr, candidate.segment(), candidate.successor(),
                    plan.transactionId(), plan.kind());
            if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve()) {
                throw new UndoWriteFatalException("free deferred undo page requirement differs from plan");
            }
            PageId firstPageId = segment.firstPageId();
            claim.bind(firstPageId);
            freePop.complete();
            UndoContext context = current == null ? new UndoContext(slotManager.rollbackSegmentId()) : current;
            context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId, UndoLogicalHead.EMPTY));
            segment.prepareAppend(plan.recordPlan());
            return preparedHandle(txn, mtr, deferred, plan, segment, context, resources);
        } catch (RuntimeException error) {
            resources.closeSuppressing(error);
            if (physical) {
                throw fatalAfterMutation("free deferred undo prepare failed after owner transition", error);
            }
            throw error;
        }
    }

    /** existing log 先 reserve 低位 FSP，再固定 first/current X 与可能的 grow/payload 页。 */
    private <T> PreparedUndoAppend<T> prepareExistingLog(Transaction txn, MiniTransaction mtr,
                                                         DeferredUndoPlan<T> deferred) {
        UndoWritePlan plan = deferred.placeholderPlan();
        UndoContext context = txn.undoContext();
        requireMatchingContext(context, plan);
        PreparedResources resources = new PreparedResources();
        boolean physical = false;
        try {
            if (plan.pagesToReserve() > 0) {
                resources.add(access.reservePages(mtr, undoSpace, plan.pagesToReserve()));
            }
            UndoLogSegment segment = access.open(mtr, plan.expectedFirstPageId(), PageLatchMode.EXCLUSIVE);
            if (!segment.appendSnapshot().equals(plan.persistentSnapshot())) {
                throw new UndoWriteStalePlanException("persistent undo snapshot changed before prepare");
            }
            if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve()) {
                throw new UndoWriteStalePlanException("undo root placement changed before prepare");
            }
            physical = true;
            segment.prepareAppend(plan.recordPlan());
            return preparedHandle(txn, mtr, deferred, plan, segment, context, resources);
        } catch (RuntimeException error) {
            resources.closeSuppressing(error);
            if (physical) {
                throw fatalAfterMutation("existing deferred undo prepare failed after physical boundary", error);
            }
            throw error;
        }
    }

    /** 构造 actual append 闭包；先做 ownership/codec shape 校验，再写 prepared 页并发布内存 logical head。 */
    private <T> PreparedUndoAppend<T> preparedHandle(Transaction txn, MiniTransaction mtr,
                                                     DeferredUndoPlan<T> deferred, UndoWritePlan plan,
                                                     UndoLogSegment segment, UndoContext context,
                                                     PreparedResources resources) {
        return new PreparedUndoAppend<>(mtr, actualOwnerships -> {
            UndoRecord actualRecord = deferred.actualRecord(actualOwnerships);
            UndoRecordWritePlan actualPlan = access.planRecord(actualRecord, deferred.keyDef(), deferred.schema());
            deferred.requireSamePhysicalShape(actualPlan);
            RollPointer pointer = segment.appendPrepared(actualPlan);
            publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
            return pointer;
        }, resources::close);
    }

    /**
     * 校验 {@code requireNoKindBinding} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param operation 传给 {@code requireNoKindBinding} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code requireNoKindBinding} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private static UndoContext requireNoKindBinding(Transaction txn, UndoWritePlan plan, String operation) {
        UndoContext current = txn.undoContext();
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || current != null && current.hasBinding(plan.kind())) {
            throw new UndoWriteStalePlanException(operation + " plan is stale");
        }
        return current;
    }

    /** prepared handle 跨方法保留的租约集合；按获取逆序关闭并聚合 suppressed。 */
    private static final class PreparedResources {
        /**
         * 本对象拥有的 {@code resources} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<AutoCloseable> resources = new ArrayList<>();
        /**
         * 记录 {@code closed} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean closed;

        private <T extends AutoCloseable> T add(T resource) {
            if (resource != null) {
                resources.add(resource);
            }
            return resource;
        }

        /**
         * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         * <p>数据流：</p>
         * <ol>
         *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
         *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
         *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
         *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
         * </ol>
         *
         */
        private void close() {
            // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
            if (closed) {
                return;
            }
            // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
            closed = true;
            // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
            RuntimeException first = null;
            for (int i = resources.size() - 1; i >= 0; i--) {
                try {
                    resources.get(i).close();
                } catch (RuntimeException error) {
                    if (first == null) {
                        first = error;
                    } else {
                        first.addSuppressed(error);
                    }
                } catch (Exception error) {
                    RuntimeException wrapped = new UndoWriteFatalException(
                            "prepared undo resource close failed", error);
                    if (first == null) {
                        first = wrapped;
                    } else {
                        first.addSuppressed(wrapped);
                    }
                }
            }
            // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
            if (first != null) {
                throw first;
            }
        }

        /**
         * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         *
         * @param primary 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
         */
        private void closeSuppressing(RuntimeException primary) {
            try {
                close();
            } catch (RuntimeException closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }
    }

    private PlanningContext planningContext(Transaction txn, UndoLogKind kind) {
        requireActiveTransaction(txn);
        TransactionId txnId = txn.transactionId();
        UndoContext context = txn.undoContext();
        UndoNo globalLast = context == null ? UndoNo.NONE : context.lastUndoNo();
        UndoLogBinding binding = context == null ? null : context.binding(kind);
        if (binding == null) {
            UndoSegmentReuseDirectory.CacheCandidate candidate = reuseDirectory.peekCache(kind).orElse(null);
            UndoSegmentReuseDirectory.FreeCandidate freeCandidate = candidate == null
                    ? reuseDirectory.peekFree().orElse(null) : null;
            UndoSegmentAcquisition acquisition = candidate != null ? UndoSegmentAcquisition.REUSE_CACHED
                    : freeCandidate != null ? UndoSegmentAcquisition.REUSE_FREE
                    : UndoSegmentAcquisition.ALLOCATE_NEW;
            PageId firstPage = candidate != null ? candidate.segment().handle().firstPageId()
                    : freeCandidate == null ? null : freeCandidate.segment().handle().firstPageId();
            return new PlanningContext(txnId, kind, acquisition, firstPage, globalLast,
                    UndoLogicalHead.EMPTY, null, candidate, freeCandidate);
        }
        UndoAppendSnapshot snapshot = binding.appendSnapshot();
        if (snapshot == null || !snapshot.transactionId().equals(txnId)
                || !snapshot.firstPageId().equals(binding.firstPageId())
                || snapshot.kind() != kind
                || !snapshot.logicalHead().equals(binding.logicalHead())) {
            throw new UndoWriteStalePlanException("transaction undo binding lacks a current append snapshot");
        }
        return new PlanningContext(txnId, kind, UndoSegmentAcquisition.APPEND_EXISTING,
                binding.firstPageId(), globalLast, binding.logicalHead(), snapshot, null, null);
    }

    /**
     * 按冻结的 UndoWritePlan 为事务创建并追加新的 undo log；先复核计划和事务快照，再在同一 MTR 内声明持久 slot、写入 undo 页并发布绑定。
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code appendAllocatedLogPlanned} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     * @throws UndoWriteFatalException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private RollPointer appendAllocatedLogPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        UndoContext current = txn.undoContext();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || (current != null && current.hasBinding(plan.kind()))) {
            throw new UndoWriteStalePlanException("new undo log plan is stale because transaction state changed");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try (RollbackSegmentSlotManager.ClaimLease claim = slotManager.reserveClaim()) {
            requirePersistentSlotFree(mtr, claim.slotId());
            UndoSpaceReservation reservation = access.reservePages(mtr, undoSpace, plan.pagesToReserve());
            claim.physicalMutationStarted();
            try (reservation) {
                    UndoLogSegment segment = access.create(mtr, undoSpace, plan.transactionId(), plan.kind());
                    if (segment.requiredNewPages(plan.recordPlan()) != plan.recordPlan().externalPageCount()) {
                        throw new UndoWriteFatalException("new undo segment page requirement differs from plan");
                    }
                    RollPointer pointer = segment.appendPlanned(plan.recordPlan());
                    PageId firstPageId = segment.firstPageId();
                    claim.bind(firstPageId);
                    claimRsegSlotAfterUndoPage(mtr, claim.slotId(), firstPageId);
                    UndoContext context = current == null
                            ? new UndoContext(slotManager.rollbackSegmentId()) : current;
                    context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId,
                            UndoLogicalHead.EMPTY));
                    publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
                    return pointer;
            } catch (RuntimeException error) {
                throw fatalAfterMutation("first undo write failed after physical mutation started", error);
            }
        }
    }

    /**
     * 用 page3 cached top 开启事务的新 kind-local undo log。cache pop、active slot claim、首页激活与首条 append
     * 处于同一业务 MTR；进入物理边界后的任何异常均 fail-stop，不能把同一 inode 同时重新发布到 cache/active。
     */
    private RollPointer appendCachedLogPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        UndoContext current = txn.undoContext();
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || (current != null && current.hasBinding(plan.kind()))) {
            throw new UndoWriteStalePlanException("cached undo plan is stale because transaction state changed");
        }
        try (RollbackSegmentSlotManager.ClaimLease claim = slotManager.reserveClaim();
             UndoSegmentReuseDirectory.CachePopLease cachePop = reuseDirectory.reserveCachePop(
                     plan.cachedCandidate())) {
            requirePersistentSlotFree(mtr, claim.slotId());
            UndoSpaceReservation reservation = plan.pagesToReserve() == 0
                    ? null : access.reservePages(mtr, undoSpace, plan.pagesToReserve());
            try (reservation) {
                claim.physicalMutationStarted();
                cachePop.physicalMutationStarted();
                try {
                    var candidate = cachePop.candidate();
                    headerRepo.moveCachedTopToActiveSlot(mtr, undoSpace, plan.kind(),
                            candidate.expectedCount(), candidate.segment().handle().firstPageId(), claim.slotId());
                    UndoLogSegment segment = access.activateCached(mtr, candidate.segment(), plan.transactionId());
                    if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve()) {
                        throw new UndoWriteFatalException("cached undo segment page requirement differs from plan");
                    }
                    RollPointer pointer = segment.appendPlanned(plan.recordPlan());
                    PageId firstPageId = segment.firstPageId();
                    claim.bind(firstPageId);
                    cachePop.complete();
                    UndoContext context = current == null
                            ? new UndoContext(slotManager.rollbackSegmentId()) : current;
                    context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId,
                            UndoLogicalHead.EMPTY));
                    publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
                    return pointer;
                } catch (RuntimeException error) {
                    throw fatalAfterMutation("cached undo first write failed after owner transition began", error);
                }
            }
        }
    }

    /**
     * 用跨 kind free FIFO 队首开启事务的新 undo log。page3 摘头/slot claim、successor.prev 清空、首页激活和首条
     * append 同属一个业务 MTR；物理边界后的异常保留 free/slot fence 并转 fatal。
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code appendFreeLogPlanned} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     * @throws UndoWriteFatalException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private RollPointer appendFreeLogPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        UndoContext current = txn.undoContext();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoNo currentGlobal = current == null ? UndoNo.NONE : current.lastUndoNo();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (!currentGlobal.equals(plan.expectedGlobalLastUndoNo())
                || (current != null && current.hasBinding(plan.kind()))) {
            throw new UndoWriteStalePlanException("free undo plan is stale because transaction state changed");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try (RollbackSegmentSlotManager.ClaimLease claim = slotManager.reserveClaim();
             UndoSegmentReuseDirectory.FreePopLease freePop = reuseDirectory.reserveFreePop(plan.freeCandidate())) {
            requirePersistentSlotFree(mtr, claim.slotId());
            UndoSpaceReservation reservation = plan.pagesToReserve() == 0
                    ? null : access.reservePages(mtr, undoSpace, plan.pagesToReserve());
            try (reservation) {
                claim.physicalMutationStarted();
                freePop.physicalMutationStarted();
                try {
                    var candidate = freePop.candidate();
                    RollbackSegmentFreeListBase expectedBase = new RollbackSegmentFreeListBase(
                            Optional.of(candidate.segment().handle().firstPageId()),
                            Optional.of(candidate.expectedTail().handle().firstPageId()),
                            candidate.expectedCount());
                    Optional<PageId> successor = candidate.successor()
                            .map(item -> item.handle().firstPageId());
                    headerRepo.moveFreeHeadToActiveSlot(mtr, undoSpace, expectedBase,
                            candidate.segment().handle().firstPageId(), successor, claim.slotId());
                    UndoLogSegment segment = access.activateFree(mtr, candidate.segment(), candidate.successor(),
                            plan.transactionId(), plan.kind());
                    if (segment.requiredNewPages(plan.recordPlan()) != plan.pagesToReserve()) {
                        throw new UndoWriteFatalException("free undo segment page requirement differs from plan");
                    }
                    RollPointer pointer = segment.appendPlanned(plan.recordPlan());
                    PageId firstPageId = segment.firstPageId();
                    claim.bind(firstPageId);
                    freePop.complete();
                    UndoContext context = current == null
                            ? new UndoContext(slotManager.rollbackSegmentId()) : current;
                    context.attach(new UndoLogBinding(plan.kind(), claim.slotId(), firstPageId,
                            UndoLogicalHead.EMPTY));
                    publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
                    return pointer;
                } catch (RuntimeException error) {
                    throw fatalAfterMutation("free undo first write failed after owner transition began", error);
                }
            }
        }
    }

    private RollPointer appendExistingPlanned(Transaction txn, MiniTransaction mtr, UndoWritePlan plan) {
        UndoContext context = txn.undoContext();
        requireMatchingContext(context, plan);
        // 预留必须先于 undo 尾页 X latch；FSP page0/page2 的页号低于普通 undo 页，反序会形成统一 latch-order 违规。
        UndoSpaceReservation reservation = plan.pagesToReserve() == 0
                ? null
                : access.reservePages(mtr, undoSpace, plan.pagesToReserve());
        try (reservation) {
            UndoLogSegment segment = access.open(mtr, plan.expectedFirstPageId(), PageLatchMode.EXCLUSIVE);
            if (!segment.appendSnapshot().equals(plan.persistentSnapshot())) {
                throw new UndoWriteStalePlanException("persistent undo append snapshot changed before execution");
            }
            int actualPages = segment.requiredNewPages(plan.recordPlan());
            if (actualPages != plan.pagesToReserve()) {
                throw new UndoWriteStalePlanException("undo root placement changed before execution");
            }
            RollPointer pointer = segment.appendPlanned(plan.recordPlan());
            publishContextAfterAppend(txn, context, plan, pointer, segment.appendSnapshot());
            return pointer;
        } catch (RuntimeException error) {
            throw fatalAfterMutation("existing undo write failed after physical mutation started", error);
        }
    }

    private void publishContextAfterAppend(Transaction txn, UndoContext context,
                                           UndoWritePlan plan, RollPointer pointer,
                                           UndoAppendSnapshot snapshot) {
        context.publishAppend(plan.kind(), plan.recordPlan().record().undoNo(), pointer, snapshot,
                plan.recordPlan().record().tableId());
        if (txn.undoContext() == null) {
            txn.setUndoContext(context);
        }
    }

    private static void requireMatchingContext(UndoContext context, UndoWritePlan plan) {
        UndoLogBinding binding = context == null ? null : context.binding(plan.kind());
        if (binding == null || !binding.firstPageId().equals(plan.expectedFirstPageId())
                || !context.lastUndoNo().equals(plan.expectedGlobalLastUndoNo())
                || !binding.logicalHead().equals(plan.expectedLogicalHead())) {
            throw new UndoWriteStalePlanException("transaction undo context changed before planned append");
        }
    }

    private static UndoWriteFatalException fatalAfterMutation(String message, RuntimeException error) {
        if (error instanceof UndoWriteFatalException fatal) {
            return fatal;
        }
        return new UndoWriteFatalException(message, error);
    }

    /**
     * 校验 {@code requireActiveTransaction} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private static void requireActiveTransaction(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("undo planning transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("undo write requires ACTIVE transaction: " + txn.state());
        }
        if (txn.transactionId().isNone()) {
            throw new TransactionStateException("undo write requires assigned transaction id");
        }
    }

    /** 规划期冻结的事务链入口。
     *
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param acquisition 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param globalLastUndoNo 参与 {@code 构造} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param logicalHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param persistentSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param cachedCandidate 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param freeCandidate 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     */
    private record PlanningContext(TransactionId transactionId, UndoLogKind kind,
                                   UndoSegmentAcquisition acquisition,
                                    PageId firstPageId, UndoNo globalLastUndoNo, UndoLogicalHead logicalHead,
                                    UndoAppendSnapshot persistentSnapshot,
                                    UndoSegmentReuseDirectory.CacheCandidate cachedCandidate,
                                    UndoSegmentReuseDirectory.FreeCandidate freeCandidate) {
        private UndoNo nextUndoNo() {
            if (globalLastUndoNo.value() == Long.MAX_VALUE) {
                throw new TransactionStateException("undo number exhausted at Long.MAX_VALUE");
            }
            return UndoNo.of(globalLastUndoNo.value() + 1L);
        }
    }

    /**
     * 提交 undo 生命周期（对齐 InnoDB {@code trx_undo_insert_cleanup} / history 挂接思想）。数据流：
     * <ul>
     *   <li>未写事务（{@code undoContext()==null}）：no-op。</li>
     *   <li>INSERT-only：合格单 fragment 段先尝试进入 INSERT cache，未接纳则进入 free FIFO，不合格段 drop；同时写正式 commit 终态。</li>
     *   <li>UPDATE-only：把 UPDATE log 标为 COMMITTED 并挂 history，留给 MVCC/purge。</li>
     *   <li>mixed：同一 MTR cache/free/drop INSERT、标记 UPDATE COMMITTED，并只写一次 commit 终态。</li>
     * </ul>
     *
     * <p><b>commit 编排</b>：{@code TransactionManager.commit()} 保持纯内存状态、**不**自动调用本方法；2.1 起
     * {@code ClusteredDmlService.commit} 先通过 {@code TransactionManager.prepareCommit(txn)} 预留提交号，
     * 再调用本方法持久化 undo 终态，最后才 {@code commit(txn)} 移出 active table。纯 insert 在此缓存或回收；
     * update/delete 原子更新 page3 base 与 first-page links 后才发布内存 history 投影，保留给 MVCC/purge。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn 提交中的事务，不能为 null。
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void onCommit(Transaction txn) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null) {
            throw new TransactionStateException("onCommit txn must not be null");
        }
        UndoContext ctx = txn.undoContext();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (ctx == null) {
            return; // 未写事务：无 undo 段
        }
        TransactionNo no = txn.transactionNo();
        if (no.isNone()) {
            throw new TransactionStateException(
                    "onCommit requires an assigned TransactionNo; call TransactionManager.prepareCommit first");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoLogBinding update = ctx.binding(UndoLogKind.UPDATE);
        if (update == null) {
            finalizer.finalizeCommit(txn, ctx, null);
            return;
        }
        HistoryEntry entry = new HistoryEntry(no, txn.transactionId(), undoSpace,
                update.firstPageId(), update.slotId(), ctx.affectedTableIds());
        // transition 在任何 page/FSP 写前取得；timeout/interrupt 不会留下半持久 commit，可由上层重试。
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try (HistoryList.AppendLease lease = history.beginAppend(entry)) {
            finalizer.finalizeCommit(txn, ctx, lease);
        }
    }

    /**
     * 持久化 XA phase one。该入口不改变 live 状态、不等待 redo fsync；稳定 storage API 必须在返回后先调用
     * {@link TransactionManager#finishPrepare(Transaction)} 发布 PREPARED，再以返回 LSN执行强制 durability wait。
     *
     * @param txn 当前可提交 ACTIVE 写事务；必须已产生至少一条普通 undo log
     * @return 覆盖全部 first-page PREPARED状态与 transaction prepare delta 的 end LSN
     * @throws TransactionStateException 无 undo、状态非法、rollback-only或提交号已经分配时抛出
     * @throws UndoFinalizationException page3/first-page证据冲突或 phase-one MTR失败时抛出
     */
    public Lsn onPrepare(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("onPrepare transaction must not be null");
        }
        UndoContext context = txn.undoContext();
        if (context == null || context.bindings().isEmpty()) {
            throw new TransactionStateException(
                    "onPrepare requires at least one ordinary undo log");
        }
        if (txn.rollbackOnly()) {
            throw new TransactionStateException(
                    "rollback-only transaction cannot prepare: " + txn.rollbackOnlyReason());
        }
        return finalizer.prepareTransaction(txn, context);
    }

    /**
     * 持久化 live prepared transaction 的 commit 决议。调用前必须通过
     * {@link TransactionManager#prepareCommitPrepared(Transaction)} 分配提交号；本方法成功后由 stable API
     * 调用 {@link TransactionManager#commitPrepared(Transaction)} 发布内存终态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 PREPARED、undo context和已分配提交号，不接受普通 ACTIVE 事务。</li>
     *     <li>UPDATE binding 存在时构造 affected-table history entry并取得有界 append lease。</li>
     *     <li>委托 finalizer 原子 drop prepared INSERT、挂接 prepared UPDATE并写 phase-two redo。</li>
     *     <li>finalizer commit 后 lease发布 history；本方法仍不移出 active table或释放 row locks。</li>
     * </ol>
     *
     * @param txn 已完成 phase one并预留提交号的 live PREPARED 事务
     * @throws TransactionStateException 状态、undo context或提交号不满足 phase-two commit条件时抛出
     * @throws UndoFinalizationException owner/history证据冲突或最终 MTR失败时抛出
     */
    public void onCommitPrepared(Transaction txn) {
        if (txn == null || txn.state() != TransactionState.PREPARED) {
            throw new TransactionStateException(
                    "onCommitPrepared requires PREPARED transaction");
        }
        UndoContext context = txn.undoContext();
        if (context == null || context.bindings().isEmpty()
                || txn.transactionNo().isNone()) {
            throw new TransactionStateException(
                    "onCommitPrepared requires ordinary undo and assigned transaction number");
        }
        onCommitPrepared(txn, context.affectedTableIds());
    }

    /**
     * 恢复期 prepared commit 变体：affected-table 集合从持久 UPDATE logical chain重建，而非依赖已丢失的
     * live side projection。live 入口也复用本方法。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn live 或 recovery 重建的 PREPARED transaction aggregate
     * @param affectedTableIds 当前 UPDATE logical chain涉及的稳定表 id；INSERT-only 可为空
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void onCommitPrepared(Transaction txn, java.util.Set<Long> affectedTableIds) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null || txn.state() != TransactionState.PREPARED
                || txn.undoContext() == null || txn.transactionNo().isNone()
                || affectedTableIds == null) {
            throw new TransactionStateException(
                    "prepared commit fields/state are invalid");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoContext context = txn.undoContext();
        UndoLogBinding update = context.binding(UndoLogKind.UPDATE);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (update == null) {
            if (!affectedTableIds.isEmpty()) {
                throw new TransactionStateException(
                        "INSERT-only prepared commit cannot publish affected history tables");
            }
            finalizer.finalizePreparedCommit(txn, context, affectedTableIds, null);
            return;
        }
        HistoryEntry entry = new HistoryEntry(
                txn.transactionNo(), txn.transactionId(), undoSpace,
                update.firstPageId(), update.slotId(), affectedTableIds);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try (HistoryList.AppendLease lease = history.beginAppend(entry)) {
            finalizer.finalizePreparedCommit(txn, context, affectedTableIds, lease);
        }
    }

    private void requirePersistentSlotFree(MiniTransaction mtr, UndoSlotId slot) {
        headerRepo.requireSlotFree(mtr, undoSpace, slot);
    }

    /**
     * 持久化 rseg slot 的 page-latch-order 例外。首写和提交清理都已经在同一 MTR 持有 undo first 页 X latch；
     * 该页可能被格式化、append 或 markCommitted 写过，不能提前释放，否则 MTR commit 盖 pageLSN 会失去 X guard。
     *
     * <p>局部无环前提：rseg header(page3) 只记录 slot->firstPageNo 映射，不会读取/等待 undo 页内容；undo
     * segment 的普通写路径也不会在持 page3 latch 时反向请求 undo page latch。因此该例外只放在 UndoLogManager
     * 的事务 undo 编排层，不下沉到 repository 的所有调用。
     */
    private void claimRsegSlotAfterUndoPage(MiniTransaction mtr, UndoSlotId slot, PageId firstPage) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo rseg slot update: page3 metadata never waits for undo page latches")) {
            headerRepo.claimSlot(mtr, undoSpace, slot, firstPage);
        }
    }
}
