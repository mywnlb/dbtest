package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnGeneration;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.ObjectName;

/**
 * CREATE TABLE 中尚未分配 table-local columnId/ordinal 的列声明。default 已由 SQL Binder
 * 按目标类型校验；Java 管理 facade 使用二参数兼容构造时，按 nullable 确定性推导旧语义。
 *
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param type 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param defaultDefinition 缺省输入语义；CONSTANT 必须已完成类型校验，IMPLICIT_NULL 仅允许 nullable 列
 * @param comment 列注释；允许空字符串且必须满足 ColumnDefinition 的持久字节上限
 * @param generation 列值生成方式；AUTO_INCREMENT 的类型/默认组合在此再次校验
 */
public record CreateColumnSpec(
        ObjectName name, ColumnTypeDefinition type,
        ColumnDefaultDefinition defaultDefinition,
        String comment, ColumnGeneration generation) {
    public CreateColumnSpec {
        if (name == null || type == null || defaultDefinition == null
                || comment == null || generation == null
                || comment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                > ColumnDefinition.MAX_COMMENT_BYTES) {
            throw new DatabaseValidationException(
                    "create column name/type/default/comment/generation are invalid");
        }
        if (!type.nullable()
                && defaultDefinition.kind() == ColumnDefaultDefinition.Kind.IMPLICIT_NULL) {
            throw new DatabaseValidationException(
                    "NOT NULL CREATE column cannot use IMPLICIT_NULL default");
        }
        if (generation == ColumnGeneration.AUTO_INCREMENT) {
            new ColumnDefinition(
                    1L, name, type, 0, defaultDefinition, comment, generation);
        }
    }

    /**
     * 保留 default-aware v2 构造入口。
     *
     * @param name 未分配 columnId 的列名
     * @param type 已校验类型
     * @param defaultDefinition 已绑定 default
     */
    public CreateColumnSpec(
            ObjectName name, ColumnTypeDefinition type,
            ColumnDefaultDefinition defaultDefinition) {
        this(name, type, defaultDefinition, "", ColumnGeneration.NONE);
    }

    /**
     * 保持 Java DDL facade 的既有调用形状；nullable 列推导 IMPLICIT_NULL，NOT NULL 列推导 REQUIRED。
     *
     * @param name 未分配 columnId 的列名
     * @param type 已完成领域范围校验的列类型
     */
    public CreateColumnSpec(ObjectName name, ColumnTypeDefinition type) {
        this(name, type, type != null && type.nullable()
                ? ColumnDefaultDefinition.implicitNull()
                : ColumnDefaultDefinition.required(), "", ColumnGeneration.NONE);
    }
}
