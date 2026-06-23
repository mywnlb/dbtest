package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Rollback segment 内事务 slot 标识（设计 §5.1/§6.3）。同一时间一个 slot 只归属一个事务，由
 * {@code RollbackSegmentSlotManager} 在事务首写时认领、commit/rollback 后回收（回收留后续片）。
 *
 * <p>本片（T1.3c）slot 只在内存目录中登记 {@code UndoSlotId -> insertUndoFirstPageId}，供事务运行时定位和
 * 测试断言；持久 rseg header slot array 与恢复期 active slot 扫描留 T1.3d。用值对象避免 slot 下标与页号、
 * segment id 混淆。
 *
 * @param value slot 下标；非负。
 */
public record UndoSlotId(int value) {

    public UndoSlotId {
        if (value < 0) {
            throw new DatabaseValidationException("undo slot id must be non-negative: " + value);
        }
    }

    /**
     * 构造一个 undo slot id。
     *
     * @param value slot 下标。
     * @return 通过校验的 undo slot id。
     */
    public static UndoSlotId of(int value) {
        return new UndoSlotId(value);
    }
}
