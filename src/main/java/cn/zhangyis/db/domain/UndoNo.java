package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务内 undo record 序号（对齐 InnoDB undo no）。单调非负，用于后续 savepoint 边界（T1.3c+）与 undo 页
 * header 的 lastUndoNo 落盘值。0 为 {@code NONE} 哨兵，表示「无 undo」；真实 undo record 的 undoNo 必须 &gt; 0
 * （由 UndoRecord 构造器与 UndoPage.appendRecord 前置校验），从而 page header lastUndoNo=0 可无歧义表「空页」。
 *
 * @param value 非负序号；0 表示无 undo。
 */
public record UndoNo(long value) {
    /** 无 undo 哨兵。 */
    public static final UndoNo NONE = new UndoNo(0);

    public UndoNo {
        if (value < 0) {
            throw new DatabaseValidationException("undo no must be non-negative: " + value);
        }
    }
    public static UndoNo of(long value) { return new UndoNo(value); }
    /** 是否为「无 undo」哨兵。 */
    public boolean isNone() { return value == 0; }
}
