package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.InsertedLobOwnership;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;

import java.util.Arrays;
import java.util.List;

/**
 * INSERT undo 的延迟发布计划。规划期以形状等价的 LOB placeholder 完成 codec、页数与 redo admission；
 * 业务 MTR 中真正分配 LOB 后，只允许替换 reference 的首页号，不能改变列序、类型、长度、页数、segment identity、
 * CRC 或 inline prefix。这样 undo root/payload 页可先固定，而 placeholder 永远不会作为可见 undo record 落盘。
 */
public final class DeferredInsertUndoPlan implements DeferredUndoPlan<InsertedLobOwnership> {

    /** 冻结 acquisition、事务逻辑头、持久页头及 placeholder 物理编码的普通 undo 计划。 */
    private final UndoWritePlan placeholderPlan;
    /** 编解码 cluster key 和 ownership 尾部所用的精确索引定义。 */
    private final IndexKeyDef keyDef;
    /** 编解码 ownership 列类型所用的精确表 schema。 */
    private final TableSchema schema;
    /** 按列序冻结的 placeholder ownership；只允许真实首页号变化。 */
    private final List<InsertedLobOwnership> placeholderOwnerships;

    /**
     * 创建 {@code DeferredInsertUndoPlan}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param placeholderPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param placeholderOwnerships 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    DeferredInsertUndoPlan(UndoWritePlan placeholderPlan, IndexKeyDef keyDef, TableSchema schema,
                           List<InsertedLobOwnership> placeholderOwnerships) {
        if (placeholderPlan == null || keyDef == null || schema == null || placeholderOwnerships == null) {
            throw new DatabaseValidationException("deferred INSERT undo plan fields must not be null");
        }
        this.placeholderPlan = placeholderPlan;
        this.keyDef = keyDef;
        this.schema = schema;
        this.placeholderOwnerships = List.copyOf(placeholderOwnerships);
    }

    /** 返回 begin admission 使用的完整 undo workload。 */
    public RedoBudgetWorkload redoWorkload() {
        return placeholderPlan.redoWorkload();
    }

    /** 返回 prepare 阶段需要固定的普通/外部 undo 页数。 */
    public int pagesToReserve() {
        return placeholderPlan.pagesToReserve();
    }

    @Override
    public UndoWritePlan placeholderPlan() {
        return placeholderPlan;
    }

    @Override
    public IndexKeyDef keyDef() {
        return keyDef;
    }

    @Override
    public TableSchema schema() {
        return schema;
    }

    /**
     * 用真实 LOB ownership 构造逻辑 undo，并逐字段复核冻结形状。允许变化的唯一字段是 reference.firstPageNo；
     * 该字段固定宽度，不改变普通 undo slot 或 external undo payload 的页数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param actualOwnerships 参与 {@code actualRecord} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code actualRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public UndoRecord actualRecord(List<InsertedLobOwnership> actualOwnerships) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (actualOwnerships == null || actualOwnerships.size() != placeholderOwnerships.size()) {
            throw new UndoWriteStalePlanException("deferred INSERT LOB ownership count changed");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        for (int i = 0; i < placeholderOwnerships.size(); i++) {
            requireSameShape(placeholderOwnerships.get(i), actualOwnerships.get(i));
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoRecord placeholder = placeholderPlan.recordPlan().record();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return UndoRecord.insert(placeholder.undoNo(), placeholder.transactionId(), placeholder.tableId(),
                placeholder.indexId(), placeholder.clusterKey(), List.copyOf(actualOwnerships),
                placeholder.secondaryMutations(), placeholder.prevRollPointer());
    }

    /** 实际编码必须与 placeholder 走相同 inline/external 分支并占用完全相同的字节数和页数。
     *
     * @param actual 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void requireSamePhysicalShape(UndoRecordWritePlan actual) {
        UndoRecordWritePlan placeholder = placeholderPlan.recordPlan();
        if (!placeholder.samePhysicalShape(actual)) {
            throw new UndoWriteStalePlanException("deferred INSERT undo physical encoding shape changed");
        }
    }

    private static void requireSameShape(InsertedLobOwnership expected, InsertedLobOwnership actual) {
        var left = expected.value();
        var right = actual.value();
        var a = left.reference();
        var b = right.reference();
        boolean same = expected.columnOrdinal() == actual.columnOrdinal()
                && left.typeId() == right.typeId()
                && a.spaceId().equals(b.spaceId())
                && a.totalLength() == b.totalLength()
                && a.pageCount() == b.pageCount()
                && a.segmentId().equals(b.segmentId())
                && a.inodeSlot() == b.inodeSlot()
                && a.crc32() == b.crc32()
                && Arrays.equals(left.inlinePrefix(), right.inlinePrefix());
        if (!same) {
            throw new UndoWriteStalePlanException("deferred INSERT LOB ownership shape/identity changed at column "
                    + expected.columnOrdinal());
        }
    }
}
