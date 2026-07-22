package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;

/**
 * 通用INPLACE manifest中的一个DROP INDEX物理退休请求。
 *
 * @param actionOrdinal 原始ALTER action零基位置
 * @param binding source committed aggregate中的精确非聚簇binding
 */
public record OnlineAlterIndexDropRequest(int actionOrdinal,
                                          IndexStorageBinding binding) {
    public OnlineAlterIndexDropRequest {
        if (actionOrdinal < 0 || binding == null) {
            throw new DatabaseValidationException(
                    "online ALTER DROP request requires ordinal/binding");
        }
    }
}
