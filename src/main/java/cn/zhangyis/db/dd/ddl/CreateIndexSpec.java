package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;

import java.util.List;

/** CREATE TABLE 内联索引声明；稳定 IndexId 由 control 一次连续预留。 */
public record CreateIndexSpec(ObjectName name, boolean unique, boolean clustered,
                              List<CreateIndexKeyPartSpec> keyParts) {
    public CreateIndexSpec {
        if (name == null || keyParts == null || keyParts.isEmpty()) {
            throw new DatabaseValidationException("create index name/key parts required");
        }
        keyParts = List.copyOf(keyParts);
    }
}
