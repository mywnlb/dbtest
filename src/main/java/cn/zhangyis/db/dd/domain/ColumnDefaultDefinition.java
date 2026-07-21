package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * 列缺省语义。CONSTANT 保存已经由 DDL binder 按目标列类型验证的 canonical SQL literal，
 * Executor 后续可按同一类型规则物化；本对象不依赖 SQL AST 或 storage codec。
 *
 * @param kind REQUIRED、IMPLICIT_NULL 或 CONSTANT
 * @param constantLiteral 仅 CONSTANT 存在；最大 4096 UTF-8 字节
 */
public record ColumnDefaultDefinition(Kind kind, Optional<String> constantLiteral) {

    /** canonical constant 的持久安全上限。 */
    public static final int MAX_LITERAL_BYTES = 4096;

    public ColumnDefaultDefinition {
        if (kind == null || constantLiteral == null
                || (kind == Kind.CONSTANT) != constantLiteral.isPresent()) {
            throw new DatabaseValidationException("column default kind/literal mismatch");
        }
        if (constantLiteral.isPresent()
                && constantLiteral.orElseThrow().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                > MAX_LITERAL_BYTES) {
            throw new DatabaseValidationException("column default literal exceeds persistent bound");
        }
    }

    /** @return NOT NULL 且无 DEFAULT 的强制输入语义 */
    public static ColumnDefaultDefinition required() {
        return new ColumnDefaultDefinition(Kind.REQUIRED, Optional.empty());
    }

    /** @return nullable 旧列与显式 DEFAULT NULL 的语义 */
    public static ColumnDefaultDefinition implicitNull() {
        return new ColumnDefaultDefinition(Kind.IMPLICIT_NULL, Optional.empty());
    }

    /** @return 已由调用方完成类型验证的常量 */
    public static ColumnDefaultDefinition constant(String canonicalLiteral) {
        if (canonicalLiteral == null) {
            throw new DatabaseValidationException("constant column default must not be null");
        }
        return new ColumnDefaultDefinition(Kind.CONSTANT, Optional.of(canonicalLiteral));
    }

    /** 列缺省类别；stable code 由各 codec 显式映射。 */
    public enum Kind {
        REQUIRED,
        IMPLICIT_NULL,
        CONSTANT
    }
}
