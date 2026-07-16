package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 稳定 table 身份；名称改变时仍保持不变，undo 和 storage binding 依赖该值。 */
public record TableId(long value) {
    public TableId {
        if (value <= 0) {
            throw new DatabaseValidationException("table id must be positive: " + value);
        }
    }

    public static TableId of(long value) {
        return new TableId(value);
    }
}
