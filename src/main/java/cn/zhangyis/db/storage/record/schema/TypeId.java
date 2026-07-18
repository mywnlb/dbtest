package cn.zhangyis.db.storage.record.schema;

/**
 * Record 层可执行物理编码的列类型（innodb-record-design §8.2 子集）。
 *
 * <p>新常量只追加在既有常量之后，避免依赖枚举顺序的诊断或测试发生无意义漂移；磁盘格式本身不持久化 ordinal。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code SMALLINT}：{@code SMALLINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code INT}：{@code INT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BIGINT}：{@code BIGINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DOUBLE}：{@code DOUBLE} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DECIMAL}：{@code DECIMAL} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code VARCHAR}：{@code VARCHAR} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BINARY}：{@code BINARY} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code VARBINARY}：{@code VARBINARY} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DATETIME}：{@code DATETIME} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 * </ul>
 */
public enum TypeId {
    /** 1 字节整数，signed/unsigned 由 {@code ColumnType} 决定。 */
    TINYINT, SMALLINT, INT, BIGINT,
    /** IEEE 754 浮点与定点十进制。 */
    FLOAT, DOUBLE, DECIMAL,
    /** 定长/变长字符与二进制字节串。 */
    CHAR, VARCHAR, BINARY, VARBINARY,
    /** 既有日期与无时区日期时间。 */
    DATE, DATETIME,
    /** 带符号毫秒 duration。 */
    TIME,
    /** 已归一化为 UTC epoch millis 的时间点。 */
    TIMESTAMP,
    /** 2B unsigned 教学年份。 */
    YEAR,
    /** 1..64 位、左对齐且尾部低位清零的 canonical bit string。 */
    BIT,
    /** schema 字典 ordinal，1..N。 */
    ENUM,
    /** schema 字典 bitmap，最多 64 个 member。 */
    SET,
    /** 最大 255B 的字符大对象。 */
    TINYTEXT,
    /** 最大 64KiB-1 的字符大对象。 */
    TEXT,
    /** 最大 16MiB-1 的字符大对象。 */
    MEDIUMTEXT,
    /** Java v1 最大 Integer.MAX_VALUE 字节的字符大对象。 */
    LONGTEXT,
    /** 最大 255B 的二进制大对象。 */
    TINYBLOB,
    /** 最大 64KiB-1 的二进制大对象。 */
    BLOB,
    /** 最大 16MiB-1 的二进制大对象。 */
    MEDIUMBLOB,
    /** Java v1 最大 Integer.MAX_VALUE 字节的二进制大对象。 */
    LONGBLOB,
    /** v1 严格 UTF-8 JSON 文本；不等同于 MySQL binary JSON。 */
    JSON
}
