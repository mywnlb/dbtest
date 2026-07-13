package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoSlotId;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存 rollback segment slot 目录（设计 §5.4/§6.3/§9.3，T1.3c 简化版）。固定单一默认
 * {@link RollbackSegmentId}，用内存 slot array 记录 {@code UndoSlotId -> insertUndoFirstPageId}，供事务运行时
 * 定位 insert undo segment 首页，并用显式状态保护认领与物理终结之间的并发窗口。
 *
 * <p><b>并发边界</b>：单把 {@link ReentrantLock} 只保护
 * {@code FREE -> RESERVED -> ACTIVE -> FINALIZING -> FREE} 短状态转换。锁内不做页分配、不访问 BufferPool、
 * 不等待 IO；调用方通过 {@link ClaimLease} 与 {@link FinalizationLease} 在锁外完成 page3/FSP/MTR 协作。
 * {@code RESERVED} 和 {@code FINALIZING} 都计为占用，避免同一 slot 在持久 owner 建立前或物理回收期间被复用。
 *
 * <p><b>持久协作</b>：本类只维护运行期投影；page3 的 claim/clear 由上层 undo 生命周期编排。终结路径必须先在
 * redo-protected MTR 中完成 segment drop + page3 owner CAS clear，再以终结租约 {@link FinalizationLease#complete()}
 * 发布内存释放。多 rseg
 * 选择策略和多 undo 表空间仍留后续切片。slot 耗尽抛 {@link UndoSlotExhaustedException}（可恢复，非致命）。
 */
public final class RollbackSegmentSlotManager {

    /** 所属 rollback segment；本片固定单一默认值，多 rseg 选择留后续片。 */
    private final RollbackSegmentId rollbackSegmentId;
    /** slot 容量；当前为固定上限，运行期终结后可复用但不动态扩容。 */
    private final int slotCapacity;
    /** slot owner array；只有 ACTIVE/FINALIZING 有非 null owner，RESERVED 尚未创建物理段。 */
    private final PageId[] slots;
    /** slot 生命周期数组；与 slots 同下标并由 lock 共同保护，是运行期是否可复用的权威状态。 */
    private final SlotLifecycleState[] states;
    /** 串行 slot 认领/查询的短锁；保护 slots 与 activeCount，不包围页分配或 IO。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 已占用 slot 计数；activeSlotCount() 直接返回，避免调用方扫全数组。 */
    private int activeCount;

    /**
     * 构造一个空的 slot 目录。
     *
     * @param rollbackSegmentId 所属 rollback segment，不能为 null。
     * @param slotCapacity      slot 容量，必须 &gt; 0。
     */
    public RollbackSegmentSlotManager(RollbackSegmentId rollbackSegmentId, int slotCapacity) {
        if (rollbackSegmentId == null) {
            throw new DatabaseValidationException("rollback segment id must not be null");
        }
        if (slotCapacity <= 0) {
            throw new DatabaseValidationException("slot capacity must be positive: " + slotCapacity);
        }
        this.rollbackSegmentId = rollbackSegmentId;
        this.slotCapacity = slotCapacity;
        this.slots = new PageId[slotCapacity];
        this.states = new SlotLifecycleState[slotCapacity];
        java.util.Arrays.fill(this.states, SlotLifecycleState.FREE);
    }

    /** 所属 rollback segment。 */
    public RollbackSegmentId rollbackSegmentId() {
        return rollbackSegmentId;
    }

    /**
     * 认领一个空闲 slot 并登记 insert undo segment 首页。数据流：加锁 → 顺序扫描首个 null slot → 写入
     * {@code firstPageId}、activeCount++ → 返回 {@link UndoSlotId}；无空槽抛 {@link UndoSlotExhaustedException}。
     * 锁内不分配页、不访问 BufferPool。
     *
     * @param insertUndoFirstPageId 已由 {@code UndoLogSegmentAccess.create} 分配的 undo segment 首页。
     * @return 认领到的 slot id。
     */
    UndoSlotId claim(PageId insertUndoFirstPageId) {
        if (insertUndoFirstPageId == null) {
            throw new DatabaseValidationException("insert undo first page id must not be null");
        }
        try (ClaimLease claim = reserveClaim()) {
            claim.bind(insertUndoFirstPageId);
            return claim.slotId();
        }
    }

    /**
     * 预留最低空闲槽并返回 RAII claim lease。预留只把 FREE 改为 RESERVED，不绑定物理 owner；因此调用方可先在
     * 锁外用 page3 S latch 验证持久槽为空，再创建 undo segment。若 lease 在 {@link ClaimLease#bind(PageId)} 前
     * 关闭，状态自动回到 FREE；绑定成功后 close 不做补偿，避免 MTR 无 content undo 时隐藏已创建物理段。
     *
     * @return 已占用一个 RESERVED 槽的 claim lease。
     * @throws UndoSlotExhaustedException 没有 FREE 槽时抛出。
     */
    ClaimLease reserveClaim() {
        lock.lock();
        try {
            for (int i = 0; i < slotCapacity; i++) {
                if (states[i] == SlotLifecycleState.FREE) {
                    states[i] = SlotLifecycleState.RESERVED;
                    activeCount++;
                    return new ClaimLease(this, UndoSlotId.of(i));
                }
            }
            throw new UndoSlotExhaustedException(rollbackSegmentId);
        } finally {
            lock.unlock();
        }
    }

    /** 将 RESERVED 槽绑定到已创建的 undo 首页并发布为 ACTIVE。 */
    private void bindClaim(UndoSlotId slot, PageId firstPageId) {
        if (firstPageId == null) {
            throw new DatabaseValidationException("claim first page id must not be null");
        }
        lock.lock();
        try {
            int idx = requireIndex(slot, "bind claim");
            if (states[idx] != SlotLifecycleState.RESERVED || slots[idx] != null) {
                throw new DatabaseValidationException("bind requires RESERVED undo slot: " + idx
                        + " state=" + states[idx]);
            }
            slots[idx] = firstPageId;
            states[idx] = SlotLifecycleState.ACTIVE;
        } finally {
            lock.unlock();
        }
    }

    /** 未绑定 claim lease 的失败补偿；只允许 RESERVED 回到 FREE。 */
    private void cancelClaim(UndoSlotId slot) {
        lock.lock();
        try {
            int idx = requireIndex(slot, "cancel claim");
            if (states[idx] != SlotLifecycleState.RESERVED || slots[idx] != null) {
                throw new DatabaseValidationException("cancel requires unbound RESERVED undo slot: " + idx
                        + " state=" + states[idx]);
            }
            states[idx] = SlotLifecycleState.FREE;
            activeCount--;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 对 ACTIVE owner 建立非阻塞终结租约。状态在短锁内改为 FINALIZING，后续重复终态命令会在触碰 page/FSP 前
     * 失败。关闭尚未进入物理修改的 lease 会恢复 ACTIVE；物理修改开始后则 fail-stop 保持 FINALIZING。
     *
     * @param slot              要终结的运行期槽。
     * @param expectedFirstPage 预期物理 owner，用于拒绝 stale terminal command。
     * @return 独占该 slot 终结资格的 RAII lease。
     */
    FinalizationLease beginFinalization(UndoSlotId slot, PageId expectedFirstPage) {
        if (expectedFirstPage == null) {
            throw new DatabaseValidationException("finalization expected first page must not be null");
        }
        lock.lock();
        try {
            int idx = requireIndex(slot, "begin finalization");
            if (states[idx] != SlotLifecycleState.ACTIVE || !expectedFirstPage.equals(slots[idx])) {
                throw new DatabaseValidationException("finalization requires matching ACTIVE undo slot " + idx
                        + ": expected=" + expectedFirstPage + ", current=" + slots[idx]
                        + ", state=" + states[idx]);
            }
            states[idx] = SlotLifecycleState.FINALIZING;
            return new FinalizationLease(this, slot, expectedFirstPage);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查询 slot 登记的 insert undo segment 首页。未认领或越界 slot 抛
     * {@link DatabaseValidationException}，不静默返回 null——否则会让 {@code UndoLogManager} 拿到 stale 定位
     * 去 reopen 错误的 undo segment。
     *
     * @param slot 要查询的 slot。
     * @return 该 slot 登记的 insert undo 首页。
     */
    public PageId insertUndoFirstPageId(UndoSlotId slot) {
        if (slot == null) {
            throw new DatabaseValidationException("slot must not be null");
        }
        lock.lock();
        try {
            int idx = slot.value();
            if (idx < 0 || idx >= slotCapacity || slots[idx] == null
                    || (states[idx] != SlotLifecycleState.ACTIVE
                    && states[idx] != SlotLifecycleState.FINALIZING)) {
                throw new DatabaseValidationException("undo slot not occupied or out of range: " + idx);
            }
            return slots[idx];
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复期重填一个 slot（0.3）。数据流：加锁 → 校验越界/未占用 → 写入 {@code firstPageId}、activeCount++ → 解锁。
     * 由启动恢复扫 page3 rseg header 后调用，把磁盘上的占用 slot 重建到内存目录。与 {@link #claim} 区别：claim
     * 自己挑空槽，restore 按磁盘记录的 slot 下标精确重填，故要求该 slot 当前为空——重复 restore（同一 slot 两次）
     * 是恢复编排 bug，必须抛 {@link DatabaseValidationException} 不静默，否则 activeCount 失衡。
     *
     * @param slot                 磁盘记录的 slot 下标。
     * @param insertUndoFirstPageId 该 slot 登记的 insert undo segment 首页。
     */
    public void restore(UndoSlotId slot, PageId insertUndoFirstPageId) {
        if (slot == null || insertUndoFirstPageId == null) {
            throw new DatabaseValidationException("restore slot/first page id must not be null");
        }
        lock.lock();
        try {
            int idx = slot.value();
            if (idx < 0 || idx >= slotCapacity) {
                throw new DatabaseValidationException("restore of out-of-range undo slot: " + idx);
            }
            if (states[idx] != SlotLifecycleState.FREE) {
                throw new DatabaseValidationException("restore of already-occupied undo slot: " + idx);
            }
            slots[idx] = insertUndoFirstPageId;
            states[idx] = SlotLifecycleState.ACTIVE;
            activeCount++;
        } finally {
            lock.unlock();
        }
    }

    /** slot 是否已占用；越界 slot 返回 false（只读查询，不抛异常）。 */
    public boolean isOccupied(UndoSlotId slot) {
        if (slot == null) {
            return false;
        }
        lock.lock();
        try {
            int idx = slot.value();
            if (idx < 0 || idx >= slotCapacity) {
                return false;
            }
            return states[idx] != SlotLifecycleState.FREE;
        } finally {
            lock.unlock();
        }
    }

    /** 已占用 slot 数量。 */
    public int activeSlotCount() {
        lock.lock();
        try {
            return activeCount;
        } finally {
            lock.unlock();
        }
    }

    /** slot 容量上限。 */
    public int slotCapacity() {
        return slotCapacity;
    }

    /** 校验 slot 非 null 且位于当前目录容量内，并返回数组下标。调用方必须已持 lock。 */
    private int requireIndex(UndoSlotId slot, String operation) {
        if (slot == null) {
            throw new DatabaseValidationException(operation + " slot must not be null");
        }
        int idx = slot.value();
        if (idx < 0 || idx >= slotCapacity) {
            throw new DatabaseValidationException(operation + " of out-of-range undo slot: " + idx);
        }
        return idx;
    }

    /**
     * 认领租约。对象由创建线程顺序使用；目录共享状态仍全部由外层显式锁保护。close 幂等，未 bind 才取消预留。
     */
    static final class ClaimLease implements AutoCloseable {
        /** 所属 slot 目录。 */
        private final RollbackSegmentSlotManager owner;
        /** 已预留的稳定槽号。 */
        private final UndoSlotId slotId;
        /** 是否已经把物理 undo 首页绑定并发布为 ACTIVE。 */
        private boolean bound;
        /** 是否已进入可能创建 segment/修改 page0 的不可逆阶段；此后异常关闭必须保留 RESERVED 防止误复用。 */
        private boolean physicalMutationStarted;
        /** 防止重复 close 或 close 后继续 bind。 */
        private boolean closed;

        private ClaimLease(RollbackSegmentSlotManager owner, UndoSlotId slotId) {
            this.owner = owner;
            this.slotId = slotId;
        }

        /** 返回当前租约预留的稳定槽号，供 page3 预检和持久登记使用。 */
        UndoSlotId slotId() {
            return slotId;
        }

        /** 段创建成功后把首页 owner 绑定到 reservation；成功后 close 不再回退状态。 */
        void bind(PageId firstPageId) {
            requireOpen("bind");
            if (bound) {
                throw new DatabaseValidationException("claim lease already bound: " + slotId.value());
            }
            owner.bindClaim(slotId, firstPageId);
            bound = true;
        }

        /** 在空间预留或 segment 创建之前标记 fail-stop 边界；标记后未 bind 的 close 也不得取消 slot。 */
        void physicalMutationStarted() {
            requireOpen("mark physical mutation");
            if (physicalMutationStarted) {
                throw new DatabaseValidationException("claim physical mutation already marked: " + slotId.value());
            }
            physicalMutationStarted = true;
        }

        /** 未绑定时释放 reservation；已绑定时只结束 guard 生命周期。 */
        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!bound && !physicalMutationStarted) {
                owner.cancelClaim(slotId);
            }
        }

        private void requireOpen(String operation) {
            if (closed) {
                throw new DatabaseValidationException(operation + " on closed claim lease: " + slotId.value());
            }
        }
    }

    /**
     * 终结租约。调用方必须先完成全部只读预检，再调用 {@link #physicalMutationStarted()}，成功提交 drop+clear MTR 后
     * 调用 {@link #complete()}。一旦声明物理修改开始，异常退出会把槽保留为 FINALIZING，防止同进程误复用。
     */
    static final class FinalizationLease implements AutoCloseable {
        /** 所属 slot 目录。 */
        private final RollbackSegmentSlotManager owner;
        /** 被终结的稳定槽号。 */
        private final UndoSlotId slotId;
        /** 获取 lease 时匹配的 undo segment 首页。 */
        private final PageId expectedFirstPage;
        /** 是否已经越过 MTR 无 content undo 的物理修改边界。 */
        private boolean physicalMutationStarted;
        /** 是否已经在持久 commit 后发布 FREE。 */
        private boolean completed;
        /** guard 是否已经关闭。 */
        private boolean closed;

        private FinalizationLease(RollbackSegmentSlotManager owner, UndoSlotId slotId,
                                  PageId expectedFirstPage) {
            this.owner = owner;
            this.slotId = slotId;
            this.expectedFirstPage = expectedFirstPage;
        }

        /** 在首个 FSP/undo/page3 写之前标记 fail-stop 边界。 */
        void physicalMutationStarted() {
            requireOpen("mark physical mutation");
            if (physicalMutationStarted) {
                throw new DatabaseValidationException("physical mutation already marked for undo slot: "
                        + slotId.value());
            }
            owner.validateFinalizingOwner(slotId, expectedFirstPage);
            physicalMutationStarted = true;
        }

        /** 持久 MTR 已成功提交后发布内存 FREE；未越过物理边界不得伪造完成。 */
        void complete() {
            requireOpen("complete finalization");
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("cannot complete undo finalization before physical mutation: "
                        + slotId.value());
            }
            owner.completeFinalization(slotId, expectedFirstPage);
            completed = true;
        }

        /** 物理修改前退出恢复 ACTIVE；物理修改后退出保留 FINALIZING；成功 complete 后无额外动作。 */
        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!completed) {
                owner.closeFinalization(slotId, expectedFirstPage, physicalMutationStarted);
            }
        }

        private void requireOpen(String operation) {
            if (closed) {
                throw new DatabaseValidationException(operation + " on closed finalization lease: "
                        + slotId.value());
            }
        }
    }

    /** 验证 lease 仍独占匹配的 FINALIZING owner。 */
    private void validateFinalizingOwner(UndoSlotId slot, PageId expectedFirstPage) {
        lock.lock();
        try {
            int idx = requireIndex(slot, "validate finalization");
            requireFinalizingOwner(idx, expectedFirstPage);
        } finally {
            lock.unlock();
        }
    }

    /** 成功持久提交后的唯一 FINALIZING -> FREE 转换。 */
    private void completeFinalization(UndoSlotId slot, PageId expectedFirstPage) {
        lock.lock();
        try {
            int idx = requireIndex(slot, "complete finalization");
            requireFinalizingOwner(idx, expectedFirstPage);
            slots[idx] = null;
            states[idx] = SlotLifecycleState.FREE;
            activeCount--;
        } finally {
            lock.unlock();
        }
    }

    /** 异常关闭终结租约：物理边界前恢复 ACTIVE，之后只校验并保留 fail-stop 状态。 */
    private void closeFinalization(UndoSlotId slot, PageId expectedFirstPage, boolean physicalMutationStarted) {
        lock.lock();
        try {
            int idx = requireIndex(slot, "close finalization");
            requireFinalizingOwner(idx, expectedFirstPage);
            if (!physicalMutationStarted) {
                states[idx] = SlotLifecycleState.ACTIVE;
            }
        } finally {
            lock.unlock();
        }
    }

    /** 调用方持 lock 时校验 FINALIZING 状态与 owner 精确匹配。 */
    private void requireFinalizingOwner(int idx, PageId expectedFirstPage) {
        if (states[idx] != SlotLifecycleState.FINALIZING || !expectedFirstPage.equals(slots[idx])) {
            throw new DatabaseValidationException("undo slot is not owned by the finalization lease: " + idx
                    + " expected=" + expectedFirstPage + ", current=" + slots[idx]
                    + ", state=" + states[idx]);
        }
    }

    /** 内存槽运行期状态；RESERVED/FINALIZING 不落盘，恢复扫描只重建 ACTIVE。 */
    private enum SlotLifecycleState {
        /** 可被下一次 first-fit 认领。 */
        FREE,
        /** 已预留槽号但尚未创建/绑定 undo segment。 */
        RESERVED,
        /** 已绑定运行期与 page3 owner，可正常 append/rollback/purge。 */
        ACTIVE,
        /** 某个终态命令已独占，是否可回退由 finalization lease 的物理边界决定。 */
        FINALIZING
    }
}
