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
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
    /** 当前 UPDATE logical head 可达记录的 undoNo→tableId；marker 发布后按新 head 裁剪，供 commit history 投影。 */
    private final NavigableMap<Long, Long> updateAffectedTables = new TreeMap<>();

    /** 构造尚未创建任何物理 log 的事务 undo 上下文。
     *
     * @param rollbackSegmentId 参与 {@code 构造} 的稳定领域标识 {@code RollbackSegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoContext(RollbackSegmentId rollbackSegmentId) {
        if (rollbackSegmentId == null) {
            throw new DatabaseValidationException("undo context rollback segment must not be null");
        }
        this.rollbackSegmentId = rollbackSegmentId;
    }

    public RollbackSegmentId rollbackSegmentId() { return rollbackSegmentId; }
    public UndoNo lastUndoNo() { return lastUndoNo; }

    /** 在 segment + page3 claim 已完成后附加一条 binding；同 kind 只允许一次。
     *
     * @param binding 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void attach(UndoLogBinding binding) {
        if (binding == null) {
            throw new DatabaseValidationException("undo binding must not be null");
        }
        if (bindings.putIfAbsent(binding.kind(), binding) != null) {
            throw new DatabaseValidationException("transaction already owns " + binding.kind() + " undo log");
        }
    }

    /** recovery 根据已核对的持久 header 恢复 binding 与事务全局高水位。
     *
     * @param binding 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param globalHighWater 参与 {@code restoreBinding} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * @param kind 选择 {@code binding} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code binding} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public UndoLogBinding binding(UndoLogKind kind) {
        requireOrdinaryKind(kind);
        return bindings.get(kind);
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * @param kind 选择 {@code hasBinding} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code hasBinding} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean hasBinding(UndoLogKind kind) {
        if (kind == UndoLogKind.TEMPORARY) {
            return false;
        }
        return binding(kind) != null;
    }

    /** 返回不可变 binding 列表；物理调用方仍须按 PageId/slot 明确排序。 */
    public Collection<UndoLogBinding> bindings() { return List.copyOf(bindings.values()); }

    /** 尚未创建指定 log 时返回 EMPTY，便于保存点把“当时不存在”表达为精确边界。
     *
     * @param kind 选择 {@code head} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code head} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public UndoLogicalHead head(UndoLogKind kind) {
        UndoLogBinding binding = binding(kind);
        return binding == null ? UndoLogicalHead.EMPTY : binding.logicalHead();
    }

    /** append MTR 成功后原子发布全局高水位和目标局部头。
     *
     * @param kind 选择 {@code publishAppend} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param undoNo 参与 {@code publishAppend} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pointer 参与 {@code publishAppend} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    void publishAppend(UndoLogKind kind, UndoNo undoNo, RollPointer pointer) {
        publishAppendInternal(kind, undoNo, pointer, null, null);
    }

    /**
     * UPDATE append MTR 成功后同时发布 logical head 与 record 所属表；INSERT table id 不进入 committed history 投影。
     *
     * @param kind    已提交 append 所属 INSERT/UPDATE undo log kind。
     * @param undoNo  本事务全局严格递增的 undo number。
     * @param pointer 新 record 的稳定 roll pointer。
     * @param tableId UPDATE/DELETE record 所属的正稳定表 id。
     * @throws DatabaseValidationException 表 id、head identity 或目标 undo binding 无效时抛出。
     */
    void publishAppend(UndoLogKind kind, UndoNo undoNo, RollPointer pointer, long tableId) {
        if (tableId <= 0L) {
            throw new DatabaseValidationException("undo append affected table id must be positive");
        }
        publishAppendInternal(kind, undoNo, pointer, null, tableId);
    }

    /** 生产 append 同时发布供下一次无 IO 规划使用的权威物理快照。
     *
     * @param kind 选择 {@code publishAppend} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param undoNo 参与 {@code publishAppend} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pointer 参与 {@code publishAppend} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param appendSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    void publishAppend(UndoLogKind kind, UndoNo undoNo, RollPointer pointer,
                       UndoAppendSnapshot appendSnapshot) {
        publishAppendInternal(kind, undoNo, pointer, appendSnapshot, null);
    }

    /**
     * 生产 UPDATE append 在 MTR commit 后同时发布 logical head、物理 append 快照和精确 table id。
     *
     * @param kind           已提交 append 所属 undo log kind；记录表投影时必须为 UPDATE。
     * @param undoNo         本事务全局严格递增的 undo number。
     * @param pointer        新 record 的稳定 roll pointer。
     * @param appendSnapshot 供下一次规划复用的已提交 first/record-page 物理快照。
     * @param tableId        本条 UPDATE/DELETE record 所属的正稳定表 id。
     * @throws DatabaseValidationException identity、binding、单调性或表 id 无效时抛出。
     */
    void publishAppend(UndoLogKind kind, UndoNo undoNo, RollPointer pointer,
                       UndoAppendSnapshot appendSnapshot, long tableId) {
        if (tableId <= 0L) {
            throw new DatabaseValidationException("undo append affected table id must be positive");
        }
        publishAppendInternal(kind, undoNo, pointer, appendSnapshot, tableId);
    }

    /**
     * 原子发布一次已经 durable/committed 的 undo append 结果到事务内存上下文；本方法不写页、不产生 redo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 head identity、目标 binding 与全局 undoNo 单调性，拒绝重复/乱序发布。</li>
     *     <li>按是否携带物理快照更新对应 INSERT/UPDATE binding 的局部 logical head。</li>
     *     <li>推进事务全局 undoNo；UPDATE 且带 table id 时记录 undoNo→table，用于 commit history 投影。</li>
     * </ol>
     *
     * @param kind           目标 INSERT 或 UPDATE undo binding 种类。
     * @param undoNo         已提交 record 的全局单调 undo number。
     * @param pointer        已提交 record 的稳定 roll pointer。
     * @param appendSnapshot 可选已提交物理尾快照；低层兼容路径可为 {@code null}。
     * @param tableId        可选 UPDATE record 表 id；INSERT 或兼容路径可为 {@code null}。
     * @throws DatabaseValidationException 字段缺失、binding 不存在、undoNo 不前进或表投影非法时抛出。
     */
    private void publishAppendInternal(UndoLogKind kind, UndoNo undoNo, RollPointer pointer,
                                       UndoAppendSnapshot appendSnapshot, Long tableId) {
        // 1. 内存发布只能消费已提交结果，identity/单调性错误表示调用顺序损坏。
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
        // 2. binding 局部 head 与可选物理快照一起更新，供下一次 append 无 IO 规划。
        UndoLogicalHead head = new UndoLogicalHead(undoNo, pointer);
        if (appendSnapshot == null) {
            binding.publishHead(head);
        } else {
            binding.publishAppend(head, appendSnapshot);
        }
        // 3. 全局高水位与 affected-table side projection 在同一事务线程内顺序发布。
        lastUndoNo = undoNo;
        if (kind == UndoLogKind.UPDATE && tableId != null) {
            updateAffectedTables.put(undoNo.value(), tableId);
        }
    }

    /**
     * 返回当前仍位于 UPDATE logical chain 中的 affected-table 去重排序快照。
     *
     * @return 按 table id 升序的不可变集合；statement/full rollback marker 已剪除的记录不再贡献表引用。
     */
    public Set<Long> affectedTableIds() {
        return Set.copyOf(new TreeSet<>(updateAffectedTables.values()));
    }

    /** full/recovery marker 已提交后，只发布所属 kind 的新局部头；全局物理高水位保持不变。
     *
     * @param kind 选择 {@code publishRollbackProgress} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param persistedHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void publishRollbackProgress(UndoLogKind kind, UndoLogicalHead persistedHead) {
        if (persistedHead == null || binding(kind) == null) {
            throw new DatabaseValidationException("rollback progress requires an existing binding and head");
        }
        binding(kind).publishHead(persistedHead);
        pruneAffectedTables(kind, persistedHead);
        savepointStack.clear();
    }

    /**
     * 根据调用参数构造 {@code createSavepoint} 对应的事务、MVCC 与锁领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @return {@code createSavepoint} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** partial rollback 的双 header marker 已同批提交后发布两个精确边界并修剪嵌套保存点。
     *
     * @param savepoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
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
        updateAffectedTables.clear();
        savepointStack.clear();
    }

    /**
     * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param savepoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
    void releaseSavepoint(TransactionSavepoint savepoint) {
        int index = requireOwnedSavepoint(savepoint);
        savepointStack.remove(index);
    }

    /**
     * 校验 {@code requireOwnedSavepoint} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param savepoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @return {@code requireOwnedSavepoint} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
        pruneAffectedTables(kind, head);
    }

    private void pruneAffectedTables(UndoLogKind kind, UndoLogicalHead head) {
        if (kind != UndoLogKind.UPDATE) {
            return;
        }
        if (head.isEmpty()) {
            updateAffectedTables.clear();
            return;
        }
        updateAffectedTables.tailMap(head.undoNo().value(), false).clear();
    }

    private static void requireOrdinaryKind(UndoLogKind kind) {
        if (kind == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("ordinary undo context kind must be INSERT or UPDATE");
        }
    }
}
