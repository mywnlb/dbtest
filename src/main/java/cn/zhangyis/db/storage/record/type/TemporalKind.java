package cn.zhangyis.db.storage.record.type;

/** 时间值种类；每个常量定义 {@link ColumnValue.TemporalValue#normalized()} 的归一化语义。 */
public enum TemporalKind {
    /** 有符号 epochDay。 */
    DATE,
    /** 无时区日期时间的有符号 epoch millis。 */
    DATETIME,
    /** 带符号毫秒 duration，不在 Record 层限制 MySQL TIME 的显示范围。 */
    TIME,
    /** 已由上层转换为 UTC 的有符号 epoch millis。 */
    TIMESTAMP,
    /** 0..65535 的无符号教学年份。 */
    YEAR
}
