package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/** 测试侧 planned undo 适配器；生产代码不保留已删除的 beforeX 兼容 API。 */
public final class UndoTestWrites {

    private UndoTestWrites() {
    }

    public static RollPointer insert(UndoLogManager manager, Transaction transaction, MiniTransaction mtr,
                                     long tableId, long indexId, List<ColumnValue> clusterKey,
                                     IndexKeyDef keyDef, TableSchema schema) {
        UndoWritePlan plan = manager.planInsert(transaction, tableId, indexId, clusterKey, keyDef, schema);
        return manager.appendPlanned(transaction, mtr, plan);
    }

    public static RollPointer update(UndoLogManager manager, Transaction transaction, MiniTransaction mtr,
                                     long tableId, long indexId, List<ColumnValue> clusterKey,
                                     List<ColumnValue> oldColumnValues, HiddenColumns oldHiddenColumns,
                                     IndexKeyDef keyDef, TableSchema schema) {
        UndoWritePlan plan = manager.planUpdate(transaction, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, keyDef, schema);
        return manager.appendPlanned(transaction, mtr, plan);
    }

    public static RollPointer delete(UndoLogManager manager, Transaction transaction, MiniTransaction mtr,
                                     long tableId, long indexId, List<ColumnValue> clusterKey,
                                     List<ColumnValue> oldColumnValues, HiddenColumns oldHiddenColumns,
                                     IndexKeyDef keyDef, TableSchema schema) {
        UndoWritePlan plan = manager.planDelete(transaction, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, keyDef, schema);
        return manager.appendPlanned(transaction, mtr, plan);
    }
}
