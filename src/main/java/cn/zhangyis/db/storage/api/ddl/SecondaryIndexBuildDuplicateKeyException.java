package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * CREATE UNIQUE INDEX backfill 发现两个 live 聚簇行具有相同非 NULL logical key。
 * 调用方必须回滚 staged index resources，不能发布 DD。
 */
public final class SecondaryIndexBuildDuplicateKeyException extends DatabaseRuntimeException {

    public SecondaryIndexBuildDuplicateKeyException(String message) {
        super(message);
    }

    public SecondaryIndexBuildDuplicateKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
