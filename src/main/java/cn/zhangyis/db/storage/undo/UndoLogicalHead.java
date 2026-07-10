package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.UndoNo;

/**
 * undo segment 当前有效反向链的持久头。{@code undoNo} 与 {@code rollPointer} 是一个不可拆分的边界：
 * 二者必须同时为空或同时非空，否则 recovery/purge 可能从错误分支开始消费物理 undo。
 *
 * <p>{@link #EMPTY} 表示事务当前没有任何有效 undo；部分回滚只移动本逻辑头，不回退物理 append 高水位，
 * 因而后续写入仍会分配更大的 undoNo，并把新记录前驱接到这里保存的 pointer。
 *
 * @param undoNo      当前逻辑链头的事务内序号；{@link UndoNo#NONE} 表示空链。
 * @param rollPointer 当前逻辑链头记录的物理地址；{@link RollPointer#NULL} 表示空链。
 */
public record UndoLogicalHead(UndoNo undoNo, RollPointer rollPointer) {

    /** 尚无有效 undo 的持久空头。 */
    public static final UndoLogicalHead EMPTY = new UndoLogicalHead(UndoNo.NONE, RollPointer.NULL);

    public UndoLogicalHead {
        if (undoNo == null || rollPointer == null) {
            throw new DatabaseValidationException("undo logical head undoNo/rollPointer must not be null");
        }
        if (undoNo.isNone() != rollPointer.isNull()) {
            throw new DatabaseValidationException(
                    "undo logical head undoNo/rollPointer must be empty or non-empty together");
        }
    }

    /** 当前是否为空链。 */
    public boolean isEmpty() {
        return undoNo.isNone();
    }
}
