package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** 不可变索引定义。table 聚合要求恰好一个 unique clustered 索引，并可同时发布多个 secondary root。
 *
 * @param id 参与 {@code 构造} 的稳定领域标识 {@code IndexId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param unique 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
 * @param clustered 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
 * @param keyParts 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
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
