package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一次 DDL 对各身份空间的连续预留数量。所有计数在一个 control generation 内持久化，避免 table/index/space
 * 分开 force 后产生难以判定的半预留状态。
 */
public record DictionaryIdRequest(int schemaCount, int tableCount, int indexCount, int spaceCount,
                                  int ddlCount, int versionCount) {
    public DictionaryIdRequest {
        if (schemaCount < 0 || tableCount < 0 || indexCount < 0 || spaceCount < 0
                || ddlCount < 0 || versionCount < 0) {
            throw new DatabaseValidationException("dictionary id reservation counts must be non-negative");
        }
        if (schemaCount + tableCount + indexCount + spaceCount + ddlCount + versionCount == 0) {
            throw new DatabaseValidationException("dictionary id reservation must request at least one identity");
        }
    }
}
