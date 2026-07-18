package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 不可变 schema 定义；默认字符语义使用稳定 ID，binder 后续可以据此补全列属性。
 *
 * @param id 参与 {@code 构造} 的稳定领域标识 {@code SchemaId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param defaultCharsetId 参与 {@code 构造} 的原始数值身份 {@code defaultCharsetId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param defaultCollationId 参与 {@code 构造} 的原始数值身份 {@code defaultCollationId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 */
public record SchemaDefinition(SchemaId id, ObjectName name, int defaultCharsetId,
                               int defaultCollationId, DictionaryVersion version) {
    public SchemaDefinition {
        if (id == null || name == null || version == null || defaultCharsetId < 0 || defaultCollationId < 0) {
            throw new DatabaseValidationException("invalid schema definition");
        }
    }
}
