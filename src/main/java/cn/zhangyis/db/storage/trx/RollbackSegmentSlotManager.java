package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoSlotId;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存 rollback segment slot 目录（设计 §5.4/§6.3/§9.3，T1.3c 简化版）。固定单一默认
 * {@link RollbackSegmentId}，用内存 slot array 记录 {@code UndoSlotId -> insertUndoFirstPageId}，供事务运行时
 * 定位 insert undo segment 首页和测试断言。
 *
 * <p><b>并发边界</b>：单把 {@link ReentrantLock} 串行「扫空槽→登记 firstPageId」短临界区。锁内不做页分配、
 * 不访问 BufferPool、不等待 IO——页分配（{@code UndoLogSegmentAccess.create}）由 {@code UndoLogManager} 在锁外
 * 完成，claim 只把已分配的首页 id 登记到空闲 slot。禁止 {@code synchronized}。
 *
 * <p><b>本片不做</b>（→ T1.3d+）：slot 回收（commit/rollback 后释放）、持久 rseg header 页格式、恢复期 active
 * slot 扫描、多 rseg 选择策略、多 undo 表空间。slot 耗尽抛 {@link UndoSlotExhaustedException}（可恢复，非致命）。
 */
public final class RollbackSegmentSlotManager {

    /** 所属 rollback segment；本片固定单一默认值，多 rseg 选择留后续片。 */
    private final RollbackSegmentId rollbackSegmentId;
    /** slot 容量；本片为固定上限，耗尽即抛异常（无回收/扩容）。 */
    private final int slotCapacity;
    /** slot array，下标 = {@link UndoSlotId#value()}；null 表空闲，非 null 表该 slot 已登记的 insert undo 首页。 */
    private final PageId[] slots;
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
    public UndoSlotId claim(PageId insertUndoFirstPageId) {
        if (insertUndoFirstPageId == null) {
            throw new DatabaseValidationException("insert undo first page id must not be null");
        }
        lock.lock();
        try {
            for (int i = 0; i < slotCapacity; i++) {
                if (slots[i] == null) {
                    slots[i] = insertUndoFirstPageId;
                    activeCount++;
                    return UndoSlotId.of(i);
                }
            }
            throw new UndoSlotExhaustedException(rollbackSegmentId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放一个已占用 slot（T1.3d）。数据流：加锁 → 校验 slot 已占用 → 置空 + activeCount-- → 解锁。
     * 由 commit（{@code UndoLogManager.onCommit}）或 rollback（{@code RollbackService} 走完链）调用：insert undo
     * 提交后不再服务一致性读、回滚后已应用完，两种情况下 slot 都可被后续事务重认领（first-fit 复用最低空槽）。
     *
     * <p>释放未占用/越界/null slot 是调用方 slot 生命周期 bug（重复释放、错配），必须抛
     * {@link DatabaseValidationException} 不静默——否则 activeCount 会失衡、空槽会被误判为占用。锁内只改内存数组，
     * 不回收 undo 页/段（undo 物理回收留 purge/truncation 片）。
     *
     * @param slot 要释放的 slot，必须当前已占用。
     */
    public void release(UndoSlotId slot) {
        if (slot == null) {
            throw new DatabaseValidationException("slot must not be null");
        }
        lock.lock();
        try {
            int idx = slot.value();
            if (idx < 0 || idx >= slotCapacity || slots[idx] == null) {
                throw new DatabaseValidationException("release of unoccupied or out-of-range undo slot: " + idx);
            }
            slots[idx] = null;
            activeCount--;
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
            if (idx < 0 || idx >= slotCapacity || slots[idx] == null) {
                throw new DatabaseValidationException("undo slot not occupied or out of range: " + idx);
            }
            return slots[idx];
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
            return slots[idx] != null;
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
}
