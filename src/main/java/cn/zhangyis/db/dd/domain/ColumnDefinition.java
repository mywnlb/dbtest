package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 表聚合内的稳定列定义；columnId 不随 ordinal 调整而复用。
 *
 * @param columnId 参与 {@code 构造} 的原始数值身份 {@code columnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param type 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
 * @param defaultDefinition 缺省输入语义；不得为 null，IMPLICIT_NULL 只能用于 nullable 列
 * @param comment 用户列注释；允许空字符串，UTF-8 字节不得超过 1024
 * @param generation 列值生成方式；AUTO_INCREMENT 只允许 NOT NULL 整数且不能声明显式默认值
 */
public record ColumnDefinition(long columnId, ObjectName name, ColumnTypeDefinition type, int ordinal,
                               ColumnDefaultDefinition defaultDefinition,
                               String comment, ColumnGeneration generation) {

    /** 列注释持久格式的 UTF-8 字节上限。 */
    public static final int MAX_COMMENT_BYTES = 1024;

    public ColumnDefinition {
        if (columnId <= 0 || ordinal < 0) {
            throw new DatabaseValidationException("column id must be positive and ordinal non-negative");
        }
        if (name == null || type == null || defaultDefinition == null
                || comment == null || generation == null
                || comment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                > MAX_COMMENT_BYTES) {
            throw new DatabaseValidationException("column name/type must not be null");
        }
        if (!type.nullable()
                && defaultDefinition.kind() == ColumnDefaultDefinition.Kind.IMPLICIT_NULL) {
            throw new DatabaseValidationException("NOT NULL column cannot have IMPLICIT_NULL default");
        }
        if (generation == ColumnGeneration.AUTO_INCREMENT
                && (type.nullable() || !integral(type.typeId())
                || defaultDefinition.kind() != ColumnDefaultDefinition.Kind.REQUIRED)) {
            throw new DatabaseValidationException(
                    "AUTO_INCREMENT column must be NOT NULL integral without explicit default");
        }
    }

    /**
     * 保留 default-aware v2 构造形状；新增字段使用空注释与普通生成方式。
     *
     * @param columnId 表内稳定列身份
     * @param name 列名
     * @param type 列类型
     * @param ordinal 连续列序号
     * @param defaultDefinition 缺省输入语义
     */
    public ColumnDefinition(
            long columnId, ObjectName name, ColumnTypeDefinition type, int ordinal,
            ColumnDefaultDefinition defaultDefinition) {
        this(columnId, name, type, ordinal, defaultDefinition, "",
                ColumnGeneration.NONE);
    }

    /**
     * 兼容旧调用点：nullable 列推导 IMPLICIT_NULL，NOT NULL 列保持 REQUIRED。
     */
    public ColumnDefinition(long columnId, ObjectName name, ColumnTypeDefinition type, int ordinal) {
        this(columnId, name, type, ordinal, type != null && type.nullable()
                ? ColumnDefaultDefinition.implicitNull() : ColumnDefaultDefinition.required(),
                "", ColumnGeneration.NONE);
    }

    private static boolean integral(DictionaryTypeId typeId) {
        return typeId == DictionaryTypeId.TINYINT
                || typeId == DictionaryTypeId.SMALLINT
                || typeId == DictionaryTypeId.INT
                || typeId == DictionaryTypeId.BIGINT;
    }
}
