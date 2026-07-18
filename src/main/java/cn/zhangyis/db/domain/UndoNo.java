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
    /**
     * 根据调用参数构造 {@code of} 对应的数据库内核值对象领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param value 由 {@code of} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @return {@code of} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static UndoNo of(long value) { return new UndoNo(value); }
    /** 是否为「无 undo」哨兵。 */
    public boolean isNone() { return value == 0; }
}
