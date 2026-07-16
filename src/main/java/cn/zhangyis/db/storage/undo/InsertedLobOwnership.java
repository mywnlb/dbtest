package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

/**
 * INSERT undo 对一条新 LOB 页链的所有权证据。columnOrdinal 只表达稳定 record schema 位置；目标列是否为
 * LOB-capable、external value type 是否匹配由持有精确 {@code TableSchema} 的 codec 复核。
 *
 * @param columnOrdinal 外部值所在的 record 列序号，必须非负；同一 UndoRecord 内还必须严格递增且唯一。
 * @param value 已发布到聚簇记录中的 external envelope；rollback 用其中 reference 定位并回收页链。
 */
public record InsertedLobOwnership(int columnOrdinal, ColumnValue.ExternalValue value) {

    public InsertedLobOwnership {
        if (columnOrdinal < 0 || value == null) {
            throw new DatabaseValidationException("inserted LOB ownership requires non-negative ordinal/value");
        }
    }
}
