package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 通用INPLACE manifest中的一个ADD INDEX物理预留请求。
 *
 * @param actionOrdinal 原始ALTER action零基位置
 * @param definition 已分配稳定index id且完成storage schema映射的非聚簇定义
 */
public record OnlineAlterIndexAddRequest(int actionOrdinal,
                                         StorageIndexDefinition definition) {
    public OnlineAlterIndexAddRequest {
        if (actionOrdinal < 0 || definition == null || definition.clustered()) {
            throw new DatabaseValidationException(
                    "online ALTER ADD request requires ordinal/non-clustered definition");
        }
    }
}
