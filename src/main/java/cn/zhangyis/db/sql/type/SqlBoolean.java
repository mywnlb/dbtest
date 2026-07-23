package cn.zhangyis.db.sql.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * SQL boolean 的三值域。该类型不使用 Java {@code null} 表达 UNKNOWN，避免优化器规则、
 * residual evaluator 和后续 FilterNode 对 NULL 真值作出不同解释。
 */
public enum SqlBoolean {
    /** 条件已确定成立；只有该值能够通过 WHERE 过滤。 */
    TRUE,
    /** 条件已确定不成立。 */
    FALSE,
    /** 条件因 SQL NULL 无法确定；WHERE 语境下不匹配，但不能与 FALSE 混为同一表达式值。 */
    UNKNOWN;

    /**
     * 按 SQL 三值逻辑计算 conjunction。
     *
     * @param other AND 右操作数；不得为 {@code null}，UNKNOWN 必须使用枚举常量显式表达
     * @return FALSE 优先，其次 UNKNOWN，只有两侧均为 TRUE 时返回 TRUE
     * @throws DatabaseValidationException 右操作数使用 Java {@code null} 时抛出
     */
    public SqlBoolean and(SqlBoolean other) {
        if (other == null) {
            throw new DatabaseValidationException(
                    "SQL boolean AND operand must not be null");
        }
        if (this == FALSE || other == FALSE) {
            return FALSE;
        }
        if (this == UNKNOWN || other == UNKNOWN) {
            return UNKNOWN;
        }
        return TRUE;
    }

    /**
     * 按 SQL 三值逻辑计算 disjunction。
     *
     * @param other OR 右操作数；不得为 {@code null}，UNKNOWN 必须显式表达
     * @return TRUE 优先，其次 UNKNOWN，只有两侧均为 FALSE 时返回 FALSE
     * @throws DatabaseValidationException 右操作数使用 Java {@code null} 时抛出
     */
    public SqlBoolean or(SqlBoolean other) {
        if (other == null) {
            throw new DatabaseValidationException(
                    "SQL boolean OR operand must not be null");
        }
        if (this == TRUE || other == TRUE) {
            return TRUE;
        }
        if (this == UNKNOWN || other == UNKNOWN) {
            return UNKNOWN;
        }
        return FALSE;
    }

    /**
     * 按 SQL 三值逻辑计算一元 NOT。
     *
     * @return TRUE/FALSE 互换，UNKNOWN 保持 UNKNOWN
     */
    public SqlBoolean not() {
        return switch (this) {
            case TRUE -> FALSE;
            case FALSE -> TRUE;
            case UNKNOWN -> UNKNOWN;
        };
    }

    /**
     * 判断当前三值结果是否通过 WHERE 过滤。
     *
     * @return 仅当前值为 TRUE 时返回 {@code true}；FALSE 和 UNKNOWN 均返回 {@code false}
     */
    public boolean matchesWhere() {
        return this == TRUE;
    }
}
