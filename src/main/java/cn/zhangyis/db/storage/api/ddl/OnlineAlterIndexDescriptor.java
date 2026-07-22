package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * descriptor chain内一个ADD/DROP索引的持久物理owner。
 *
 * @param action manifest方向
 * @param actionOrdinal 原始action零基位置
 * @param indexBinding root与两个segment的精确binding
 */
public record OnlineAlterIndexDescriptor(OnlineAlterIndexDescriptorAction action,
                                         int actionOrdinal,
                                         IndexStorageBinding indexBinding) {
    public OnlineAlterIndexDescriptor {
        if (action == null || actionOrdinal < 0 || indexBinding == null) {
            throw new DatabaseValidationException(
                    "invalid online ALTER index descriptor");
        }
    }
}
