package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** 不可变索引定义。table 聚合要求恰好一个 unique clustered 索引，并可同时发布多个 secondary root。 */
public record IndexDefinition(IndexId id, ObjectName name, boolean unique, boolean clustered,
                              List<IndexKeyPart> keyParts) {
    public IndexDefinition {
        if (id == null || name == null || keyParts == null || keyParts.isEmpty()) {
            throw new DatabaseValidationException("index id/name/key parts must not be null or empty");
        }
        keyParts = List.copyOf(keyParts);
        if (clustered && !unique) {
            throw new DatabaseValidationException("clustered primary index must be unique");
        }
    }
}
