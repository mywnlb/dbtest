package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** DDL 交给 storage 的有序列定义；columnId 是 DD 稳定身份，ordinal 决定物理 record 位置。
 *
 * @param columnId 参与 {@code 构造} 的原始数值身份 {@code columnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param name 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
 * @param type 选择 {@code 构造} 分支的 {@code StorageColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 */
public record StorageColumnDefinition(long columnId, String name, int ordinal, StorageColumnType type) {
    public StorageColumnDefinition {
        if (columnId <= 0 || name == null || name.isBlank() || ordinal < 0 || type == null) {
            throw new DatabaseValidationException("invalid storage column definition");
        }
    }
}
