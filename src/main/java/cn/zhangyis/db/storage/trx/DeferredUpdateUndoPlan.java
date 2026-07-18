package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.LobVersionOwnership;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;

import java.util.Arrays;
import java.util.List;

/**
 * LOB-aware UPDATE undo 的延迟发布计划。规划期使用形状等价的 rollback-new placeholder external envelope；
 * 业务 MTR 写出真实 LOB 后，只允许 reference firstPageNo 变化，旧 image、purge flag、secondary tail、长度、页数、
 * segment、CRC 和 inline prefix 均必须保持不变。
 */
public final class DeferredUpdateUndoPlan implements DeferredUndoPlan<LobVersionOwnership> {

    /** 冻结 acquisition、logical head、placeholder 编码和 redo workload 的普通 undo 计划。 */
    private final UndoWritePlan placeholderPlan;
    /** 解码聚簇 key 使用的 exact-version key definition。 */
    private final IndexKeyDef keyDef;
    /** 解码全量旧 image 与 LV envelope 使用的 exact-version schema。 */
    private final TableSchema schema;
    /** 按 ordinal 冻结的 placeholder LV ownership；actual 只允许新链首页号变化。 */
    private final List<LobVersionOwnership> placeholderOwnerships;

    /**
     * 创建有效 deferred UPDATE 计划；对象发布后所有集合均不可变。
     *
     * @param placeholderPlan       已按 placeholder record 冻结的普通 undo 写计划。
     * @param keyDef               exact-version 聚簇 key definition。
     * @param schema               exact-version 完整表 schema。
     * @param placeholderOwnerships 按 ordinal 递增的 LV ownership placeholder。
     * @throws DatabaseValidationException 任一字段缺失时抛出；失败不修改事务 undo context。
     */
    DeferredUpdateUndoPlan(UndoWritePlan placeholderPlan, IndexKeyDef keyDef, TableSchema schema,
                           List<LobVersionOwnership> placeholderOwnerships) {
        if (placeholderPlan == null || keyDef == null || schema == null || placeholderOwnerships == null) {
            throw new DatabaseValidationException("deferred UPDATE undo plan fields must not be null");
        }
        this.placeholderPlan = placeholderPlan;
        this.keyDef = keyDef;
        this.schema = schema;
        this.placeholderOwnerships = List.copyOf(placeholderOwnerships);
    }

    /**
     * 返回业务 MTR admission 使用的 undo workload。
     *
     * @return placeholder 与 actual 共享的 root/payload/record redo 上界。
     */
    public RedoBudgetWorkload redoWorkload() { return placeholderPlan.redoWorkload(); }

    /**
     * 返回 prepare 阶段需要预留的 undo root grow/external payload 页数。
     *
     * @return 非负页数；actual ownership 不允许改变该值。
     */
    public int pagesToReserve() { return placeholderPlan.pagesToReserve(); }

    @Override
    public UndoWritePlan placeholderPlan() { return placeholderPlan; }

    @Override
    public IndexKeyDef keyDef() { return keyDef; }

    @Override
    public TableSchema schema() { return schema; }

    /**
     * 用真实 rollback-new external envelope 构造 actual UPDATE undo，并逐项复核冻结 ownership 形状。
     *
     * @param actualOwnerships 实际新 LOB allocation 产生的 LV ownership；顺序和数量必须与 placeholder 相同。
     * @return 旧 image、secondary tail 和逻辑 identity 保持不变的 actual UPDATE_ROW。
     * @throws UndoWriteStalePlanException ownership 数量或除 firstPageNo 外的任一字段发生变化时抛出。
     */
    @Override
    public UndoRecord actualRecord(List<LobVersionOwnership> actualOwnerships) {
        if (actualOwnerships == null || actualOwnerships.size() != placeholderOwnerships.size()) {
            throw new UndoWriteStalePlanException("deferred UPDATE LOB ownership count changed");
        }
        for (int i = 0; i < placeholderOwnerships.size(); i++) {
            requireSameShape(placeholderOwnerships.get(i), actualOwnerships.get(i));
        }
        UndoRecord placeholder = placeholderPlan.recordPlan().record();
        return UndoRecord.update(placeholder.undoNo(), placeholder.transactionId(), placeholder.tableId(),
                placeholder.indexId(), placeholder.clusterKey(), placeholder.oldColumnValues(),
                placeholder.oldHiddenColumns(), List.copyOf(actualOwnerships),
                placeholder.secondaryMutations(), placeholder.prevRollPointer());
    }

    /**
     * 确认 actual record 没有改变 undo slot/external payload 的物理分支和长度。
     *
     * @param actual actual ownership 完成编码后的 record 写计划。
     * @throws UndoWriteStalePlanException 编码长度、inline/external 分支或 payload 页数变化时抛出。
     */
    @Override
    public void requireSamePhysicalShape(UndoRecordWritePlan actual) {
        if (!placeholderPlan.recordPlan().samePhysicalShape(actual)) {
            throw new UndoWriteStalePlanException("deferred UPDATE undo physical encoding shape changed");
        }
    }

    /**
     * 比较 placeholder/actual LV ownership，只允许 rollback-new reference firstPageNo 改变。
     *
     * @param expected 规划期进入 undo codec 和 redo admission 的 placeholder ownership。
     * @param actual   当前业务 MTR 新 LOB allocation 产生的真实 ownership。
     * @throws UndoWriteStalePlanException ordinal/action、类型、segment、长度、页数、CRC 或 prefix 任一漂移时抛出。
     */
    private static void requireSameShape(LobVersionOwnership expected, LobVersionOwnership actual) {
        if (expected.columnOrdinal() != actual.columnOrdinal()
                || expected.purgeOldValue() != actual.purgeOldValue()
                || expected.rollbackNewValue().isPresent() != actual.rollbackNewValue().isPresent()) {
            throw new UndoWriteStalePlanException("deferred UPDATE LOB action shape changed at column "
                    + expected.columnOrdinal());
        }
        if (expected.rollbackNewValue().isEmpty()) {
            return;
        }
        var left = expected.rollbackNewValue().orElseThrow();
        var right = actual.rollbackNewValue().orElseThrow();
        var a = left.reference();
        var b = right.reference();
        boolean same = left.typeId() == right.typeId()
                && a.spaceId().equals(b.spaceId())
                && a.totalLength() == b.totalLength()
                && a.pageCount() == b.pageCount()
                && a.segmentId().equals(b.segmentId())
                && a.inodeSlot() == b.inodeSlot()
                && a.crc32() == b.crc32()
                && Arrays.equals(left.inlinePrefix(), right.inlinePrefix());
        if (!same) {
            throw new UndoWriteStalePlanException("deferred UPDATE LOB reference shape changed at column "
                    + expected.columnOrdinal());
        }
    }
}
