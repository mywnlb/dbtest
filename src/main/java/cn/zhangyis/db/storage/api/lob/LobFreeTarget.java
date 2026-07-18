package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.type.ColumnValue;

/**
 * 一条待批量释放的 external LOB ownership。ordinal 只用于损坏诊断；物理授权来自批次的 authoritative segment。
 *
 * @param columnOrdinal exact-version schema ordinal，必须非负。
 * @param columnType    解释 external envelope 和完整 payload 的列类型。
 * @param externalValue 待释放的稳定 external envelope。
 */
public record LobFreeTarget(int columnOrdinal, ColumnType columnType,
                            ColumnValue.ExternalValue externalValue) {

    /**
     * 校验批量释放目标的基本值对象形状。这里只冻结“哪一列、按什么类型解释、释放哪个 external envelope”；
     * segment 授权、完整页链和 CRC 必须由 {@link LobStorage} 在任何 FSP 修改前统一复核。
     *
     * @param columnOrdinal exact-version schema 中的列序号；必须大于等于零，用于错误定位和稳定批次排序。
     * @param columnType    目标列的精确存储类型；必须属于 TEXT/BLOB/JSON family，具体约束由 LobStorage 校验。
     * @param externalValue undo ownership 保存的 external envelope；不能为 {@code null}，且不能由调用方据此猜 segment。
     * @throws DatabaseValidationException ordinal 为负数或任一对象字段缺失时抛出；失败不读取页或修改 FSP。
     */
    public LobFreeTarget {
        if (columnOrdinal < 0 || columnType == null || externalValue == null) {
            throw new DatabaseValidationException("LOB free target fields are invalid");
        }
    }
}
