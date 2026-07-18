package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;

/** CREATE INDEX 键列按名称引用；binder/coordinator 在持有 MDL 后解析为稳定 columnId。
 *
 * @param columnName 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param order 选择 {@code 构造} 分支的 {@code IndexOrder} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param prefixBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
 */
public record CreateIndexKeyPartSpec(ObjectName columnName, IndexOrder order, int prefixBytes) {
    public CreateIndexKeyPartSpec {
        if (columnName == null || order == null || prefixBytes < 0) {
            throw new DatabaseValidationException("invalid create index key part");
        }
    }
}
