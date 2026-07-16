package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;

/** CREATE INDEX 键列按名称引用；binder/coordinator 在持有 MDL 后解析为稳定 columnId。 */
public record CreateIndexKeyPartSpec(ObjectName columnName, IndexOrder order, int prefixBytes) {
    public CreateIndexKeyPartSpec {
        if (columnName == null || order == null || prefixBytes < 0) {
            throw new DatabaseValidationException("invalid create index key part");
        }
    }
}
