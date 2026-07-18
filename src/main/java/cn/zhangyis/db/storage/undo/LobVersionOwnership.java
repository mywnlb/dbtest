package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.Optional;

/**
 * UPDATE/DELETE undo 对一列 external LOB 的版本 ownership。
 *
 * @param columnOrdinal   exact-version schema 中的列序号；同一 undo record 内必须严格递增。
 * @param purgeOldValue   committed purge 是否释放 {@code oldColumnValues[columnOrdinal]} 中的旧 external 链。
 * @param rollbackNewValue rollback marker 是否释放前向 UPDATE 新建的 external 链；DELETE 必须为空。
 */
public record LobVersionOwnership(int columnOrdinal, boolean purgeOldValue,
                                  Optional<ColumnValue.ExternalValue> rollbackNewValue) {

    /**
     * 校验 ownership 至少表达一个反向动作，并冻结可选新链引用。该对象不自行读取 schema 或 LOB 页；
     * {@link UndoRecord} 负责与 old image 交叉校验，codec/rollback/purge 再以 exact metadata 校验物理类型和 segment。
     *
     * @param columnOrdinal    schema ordinal，必须非负。
     * @param purgeOldValue    是否由 committed purge 回收旧 image 中的链。
     * @param rollbackNewValue 是否由 rollback marker 回收前向写入的新链。
     * @throws DatabaseValidationException ordinal 无效、Optional 缺失或两个动作都为空时抛出。
     */
    public LobVersionOwnership {
        if (columnOrdinal < 0 || rollbackNewValue == null) {
            throw new DatabaseValidationException("LOB version ownership fields are invalid");
        }
        if (!purgeOldValue && rollbackNewValue.isEmpty()) {
            throw new DatabaseValidationException("LOB version ownership must define purge-old or rollback-new");
        }
    }
}
