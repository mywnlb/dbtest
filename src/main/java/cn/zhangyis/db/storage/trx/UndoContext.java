package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;

/**
 * 事务聚合内部的 undo 子状态。INSERT/UPDATE 各有独立 slot、segment 和局部逻辑链；事务只共享全局单调
 * {@code lastUndoNo}，rollback 用两个局部头的 undoNo 归并真实 DML 逆序。对象归单事务 writer 所有。
 */
public final class UndoContext {

    /** 所属 rollback segment；当前组合根固定一个 rseg，多 rseg 调度留后续切片。 */
    private final RollbackSegmentId rollbackSegmentId;
    /** 每种普通 undo 最多一条绑定；TEMPORARY 不进入本普通 undo context。 */
    private final EnumMap<UndoLogKind, UndoLogBinding> bindings = new EnumMap<>(UndoLogKind.class);
    /** 事务物理 append 全局高水位；partial rollback 永不回退，防止 detached undoNo 被复用。 */
    private UndoNo lastUndoNo = UndoNo.NONE;
    /** 运行期保存点栈；每项同时捕获 INSERT/UPDATE 两个精确局部头。 */
    private final List<TransactionSavepoint> savepointStack = new ArrayList<>();
    /** 同一 context 内保存点创建序号，只用于诊断与稳定归属。 */
    private long nextSavepointSequence;

    /** 构造尚未创建任何物理 log 的事务 undo 上下文。 */
    public UndoContext(RollbackSegmentId rollbackSegmentId) {
        if (rollbackSegmentId == null) {
            throw new DatabaseValidationException("undo context rollback segment must not be null");
        }
        this.rollbackSegmentId = rollbackSegmentId;
    }

    public RollbackSegmentId rollbackSegmentId() { return rollbackSegmentId; }
    public UndoNo lastUndoNo() { return lastUndoNo; }

    /** 在 segment + page3 claim 已完成后附加一条 binding；同 kind 只允许一次。 */
    void attach(UndoLogBinding binding) {
        if (binding == null) {
            throw new DatabaseValidationException("undo binding must not be null");
        }
        if (bindings.putIfAbsent(binding.kind(), binding) != null) {
            throw new DatabaseValidationException("transaction already owns " + binding.kind() + " undo log");
        }
    }

    /** recovery 根据已核对的持久 header 恢复 binding 与事务全局高水位。 */
    void restoreBinding(UndoLogBinding binding, UndoNo globalHighWater) {
        if (binding == null || globalHighWater == null
                || globalHighWater.value() < binding.logicalHead().undoNo().value()) {
            throw new DatabaseValidationException("invalid recovered undo binding/high-water");
        }
        attach(binding);
        if (globalHighWater.value() > lastUndoNo.value()) {
            lastUndoNo = globalHighWater;
        }
    }

    public UndoLogBinding binding(UndoLogKind kind) {
        requireOrdinaryKind(kind);
        return bindings.get(kind);
    }

    public boolean hasBinding(UndoLogKind kind) {
        if (kind == UndoLogKind.TEMPORARY) {
            return false;
        }
        return binding(kind) != null;
    }

    /** 返回不可变 binding 列表；物理调用方仍须按 PageId/slot 明确排序。 */
    public Collection<UndoLogBinding> bindings() { return List.copyOf(bindings.values()); }

    /** 尚未创建指定 log 时返回 EMPTY，便于保存点把“当时不存在”表达为精确边界。 */
    public UndoLogicalHead head(UndoLogKind kind) {
        UndoLogBinding binding = binding(kind);
        return binding == null ? UndoLogicalHead.EMPTY : binding.logicalHead();
    }

    /** append MTR 成功后原子发布全局高水位和目标局部头。 */
    void publishAppend(UndoLogKind kind, UndoNo undoNo, RollPointer pointer) {
        publishAppend(kind, undoNo, pointer, null);
    }

    /** 生产 append 同时发布供下一次无 IO 规划使用的权威物理快照。 */
    void publishAppend(UndoLogKind kind, UndoNo undoNo, RollPointer pointer,
                       UndoAppendSnapshot appendSnapshot) {
        if (undoNo == null || pointer == null) {
            throw new DatabaseValidationException("undo append publication fields must not be null");
        }
        UndoLogBinding binding = binding(kind);
        if (binding == null) {
            throw new DatabaseValidationException("transaction has no " + kind + " undo binding");
        }
        if (undoNo.value() <= lastUndoNo.value()) {
            throw new DatabaseValidationException("global undo number must advance: current="
                    + lastUndoNo.value() + ", next=" + undoNo.value());
        }
        UndoLogicalHead head = new UndoLogicalHead(undoNo, pointer);
        if (appendSnapshot == null) {
            binding.publishHead(head);
        } else {
            binding.publishAppend(head, appendSnapshot);
        }
        lastUndoNo = undoNo;
    }

    /** full/recovery marker 已提交后，只发布所属 kind 的新局部头；全局物理高水位保持不变。 */
    void publishRollbackProgress(UndoLogKind kind, UndoLogicalHead persistedHead) {
        if (persistedHead == null || binding(kind) == null) {
            throw new DatabaseValidationException("rollback progress requires an existing binding and head");
        }
        binding(kind).publishHead(persistedHead);
        savepointStack.clear();
    }

    TransactionSavepoint createSavepoint(Transaction txn) {
        if (txn == null || txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("savepoint requires an ACTIVE transaction");
        }
        if (txn.undoContext() != null && txn.undoContext() != this) {
            throw new DatabaseValidationException("transaction is bound to a different undo context");
        }
        TransactionSavepoint savepoint = new TransactionSavepoint(txn,
                head(UndoLogKind.INSERT), head(UndoLogKind.UPDATE), nextSavepointSequence++);
        savepointStack.add(savepoint);
        return savepoint;
    }

    /** partial rollback 的双 header marker 已同批提交后发布两个精确边界并修剪嵌套保存点。 */
    void completeRollbackToSavepoint(TransactionSavepoint savepoint) {
        int index = requireOwnedSavepoint(savepoint);
        publishExistingHead(UndoLogKind.INSERT, savepoint.insertHead());
        publishExistingHead(UndoLogKind.UPDATE, savepoint.updateHead());
        for (int i = savepointStack.size() - 1; i > index; i--) {
            savepointStack.remove(i);
        }
    }

    /** statement rollback 回到两条 log 都为空的边界，不回退全局 append 高水位。 */
    void completeRollbackToEmptyBoundary() {
        for (UndoLogBinding binding : bindings.values()) {
            binding.publishHead(UndoLogicalHead.EMPTY);
        }
        savepointStack.clear();
    }

    void releaseSavepoint(TransactionSavepoint savepoint) {
        int index = requireOwnedSavepoint(savepoint);
        for (int i = savepointStack.size() - 1; i >= index; i--) {
            savepointStack.remove(i);
        }
    }

    int requireOwnedSavepoint(TransactionSavepoint savepoint) {
        if (savepoint == null) {
            throw new DatabaseValidationException("transaction savepoint must not be null");
        }
        int index = savepointStack.indexOf(savepoint);
        if (index < 0) {
            throw new DatabaseValidationException("savepoint does not belong to this undo context");
        }
        return index;
    }

    int savepointCount() { return savepointStack.size(); }

    private void publishExistingHead(UndoLogKind kind, UndoLogicalHead head) {
        UndoLogBinding binding = bindings.get(kind);
        if (binding == null) {
            if (!head.isEmpty()) {
                throw new DatabaseValidationException("savepoint references missing " + kind + " undo log");
            }
            return;
        }
        binding.publishHead(head);
    }

    private static void requireOrdinaryKind(UndoLogKind kind) {
        if (kind == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("ordinary undo context kind must be INSERT or UPDATE");
        }
    }
}
