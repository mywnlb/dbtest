package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * undo record（类型判别命令对象，设计 §5.5/§6.5）。
 *
 * <ul>
 *   <li><b>INSERT_ROW</b>（T1.3a 起）：保存按 cluster key 物理删除未提交插入所需信息；rollback 用
 *       {@code indexId}+{@code clusterKey} 反查聚簇记录，并以 {@code transactionId}+roll pointer 校验
 *       「找到的是本事务的未提交插入」。不带旧 image（insert 无旧版本）。</li>
 *   <li><b>UPDATE_ROW</b>（T1.3e 起）：额外保存**全量旧 image**——{@code oldColumnValues}（更新前全列值）+
 *       {@code oldHiddenColumns}（更新前 DB_TRX_ID/DB_ROLL_PTR）。rollback 用旧 image 整记录恢复；T1.4 MVCC 经
 *       {@code oldHiddenColumns.dbRollPtr()} 串记录版本链（§6.5 第397行：phase-1 存全量旧 image，简化 vs InnoDB
 *       changed-columns diff）。</li>
 * </ul>
 *
 * <p><b>两条链区分</b>（设计 §5.3/§7.5）：{@code prevRollPointer}=**事务回滚链**（RollbackService 反向走，
 * 串本事务所有 undo by undoNo）；**记录版本链**=聚簇记录 {@code DB_ROLL_PTR} → 本 update undo →
 * {@code oldHiddenColumns.dbRollPtr()}（上一版本）。二者用不同字段表达，勿混用。
 *
 * <p>{@code DELETE_MARK} 仍拒绝（→ T1.3f）。{@code undoNo} 必须 &gt; 0（非 {@code NONE}）。
 *
 * @param type            undo 类型（INSERT_ROW 或 UPDATE_ROW）。
 * @param undoNo          事务内序号（&gt; 0）。
 * @param transactionId   写入该 undo 的事务 id。
 * @param tableId         表 id（rollback 定位用）。
 * @param indexId         聚簇索引 id（rollback 定位用）。
 * @param clusterKey      主键列值，顺序对应 IndexKeyDef.parts()；可含 {@link ColumnValue.NullValue}。
 * @param oldColumnValues UPDATE_ROW 的更新前全列值（按 schema 列序）；INSERT_ROW 必为 null。
 * @param oldHiddenColumns UPDATE_ROW 的更新前隐藏列；INSERT_ROW 必为 null。
 * @param prevRollPointer 事务反向 undo 链前驱（= 写入时 ctx.lastRollPointer）。
 */
public record UndoRecord(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                         long tableId, long indexId, List<ColumnValue> clusterKey,
                         List<ColumnValue> oldColumnValues, HiddenColumns oldHiddenColumns,
                         RollPointer prevRollPointer) {

    public UndoRecord {
        if (type == null || undoNo == null || transactionId == null
                || clusterKey == null || prevRollPointer == null) {
            throw new DatabaseValidationException("undo record fields must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo record undoNo must be > 0 (not NONE)");
        }
        if (clusterKey.isEmpty()) {
            throw new DatabaseValidationException("undo record clusterKey must not be empty");
        }
        clusterKey = List.copyOf(clusterKey);
        switch (type) {
            case INSERT_ROW -> {
                // insert 无旧版本：携带旧 image 是构造错误
                if (oldColumnValues != null || oldHiddenColumns != null) {
                    throw new DatabaseValidationException("INSERT_ROW undo must not carry old image");
                }
            }
            // UPDATE_ROW / DELETE_MARK 都带删改前的旧 image（删除前是存活版本：列不变 + 旧隐藏列）
            case UPDATE_ROW, DELETE_MARK -> {
                if (oldColumnValues == null || oldHiddenColumns == null) {
                    throw new DatabaseValidationException(
                            type + " undo requires old image (oldColumnValues + oldHiddenColumns)");
                }
                if (oldColumnValues.isEmpty()) {
                    throw new DatabaseValidationException(type + " undo oldColumnValues must not be empty");
                }
                oldColumnValues = List.copyOf(oldColumnValues);
            }
        }
    }

    /** 构造 INSERT_ROW undo（不带旧 image）。 */
    public static UndoRecord insert(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                null, null, prevRollPointer);
    }

    /** 构造 UPDATE_ROW undo（带全量旧 image：旧全列值 + 旧隐藏列）。 */
    public static UndoRecord update(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.UPDATE_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, prevRollPointer);
    }

    /**
     * 构造 DELETE_MARK undo（带删除前**存活**版本的全量旧 image：列不变 + 旧隐藏列）。rollback 用它取消删除标记并
     * 还原旧隐藏列；MVCC 旧版本遍历与 UPDATE 同路。本片不存 old delete flag（旧状态隐含 false，见 slice 决策 1）。
     */
    public static UndoRecord deleteMark(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                        List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                        HiddenColumns oldHiddenColumns, RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.DELETE_MARK, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, prevRollPointer);
    }
}
