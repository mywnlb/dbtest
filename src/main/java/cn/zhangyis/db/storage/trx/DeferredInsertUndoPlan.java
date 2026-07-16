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
public final class DeferredInsertUndoPlan {

    /** 冻结 acquisition、事务逻辑头、持久页头及 placeholder 物理编码的普通 undo 计划。 */
    private final UndoWritePlan placeholderPlan;
    /** 编解码 cluster key 和 ownership 尾部所用的精确索引定义。 */
    private final IndexKeyDef keyDef;
    /** 编解码 ownership 列类型所用的精确表 schema。 */
    private final TableSchema schema;
    /** 按列序冻结的 placeholder ownership；只允许真实首页号变化。 */
    private final List<InsertedLobOwnership> placeholderOwnerships;

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

    UndoWritePlan placeholderPlan() {
        return placeholderPlan;
    }

    IndexKeyDef keyDef() {
        return keyDef;
    }

    TableSchema schema() {
        return schema;
    }

    /**
     * 用真实 LOB ownership 构造逻辑 undo，并逐字段复核冻结形状。允许变化的唯一字段是 reference.firstPageNo；
     * 该字段固定宽度，不改变普通 undo slot 或 external undo payload 的页数。
     */
    UndoRecord actualRecord(List<InsertedLobOwnership> actualOwnerships) {
        if (actualOwnerships == null || actualOwnerships.size() != placeholderOwnerships.size()) {
            throw new UndoWriteStalePlanException("deferred INSERT LOB ownership count changed");
        }
        for (int i = 0; i < placeholderOwnerships.size(); i++) {
            requireSameShape(placeholderOwnerships.get(i), actualOwnerships.get(i));
        }
        UndoRecord placeholder = placeholderPlan.recordPlan().record();
        return UndoRecord.insert(placeholder.undoNo(), placeholder.transactionId(), placeholder.tableId(),
                placeholder.indexId(), placeholder.clusterKey(), List.copyOf(actualOwnerships),
                placeholder.prevRollPointer());
    }

    /** 实际编码必须与 placeholder 走相同 inline/external 分支并占用完全相同的字节数和页数。 */
    void requireSamePhysicalShape(UndoRecordWritePlan actual) {
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
