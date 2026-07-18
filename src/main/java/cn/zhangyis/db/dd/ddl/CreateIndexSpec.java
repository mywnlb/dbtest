package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;

import java.util.List;

/** CREATE TABLE 内联索引声明；稳定 IndexId 由 control 一次连续预留。
 *
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param unique 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
 * @param clustered 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
 * @param keyParts 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record CreateIndexSpec(ObjectName name, boolean unique, boolean clustered,
                              List<CreateIndexKeyPartSpec> keyParts) {
    public CreateIndexSpec {
        if (name == null || keyParts == null || keyParts.isEmpty()) {
            throw new DatabaseValidationException("create index name/key parts required");
        }
        keyParts = List.copyOf(keyParts);
    }
}
