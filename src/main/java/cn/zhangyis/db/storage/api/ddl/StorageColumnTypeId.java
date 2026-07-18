package cn.zhangyis.db.storage.api.ddl;

/** storage.api 对上暴露的稳定列类型集合；不泄漏 record 包的物理 codec 类型。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code SMALLINT}：{@code SMALLINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code INT}：{@code INT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BIGINT}：{@code BIGINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code FLOAT}：{@code FLOAT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DOUBLE}：{@code DOUBLE} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DECIMAL}：{@code DECIMAL} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code VARCHAR}：{@code VARCHAR} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BINARY}：{@code BINARY} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code VARBINARY}：{@code VARBINARY} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DATE}：{@code DATE} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DATETIME}：{@code DATETIME} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TIME}：{@code TIME} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TIMESTAMP}：{@code TIMESTAMP} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code YEAR}：{@code YEAR} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code ENUM}：{@code ENUM} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code SET}：{@code SET} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TINYTEXT}：{@code TINYTEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TEXT}：{@code TEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code MEDIUMTEXT}：{@code MEDIUMTEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code LONGTEXT}：{@code LONGTEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BLOB}：{@code BLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code MEDIUMBLOB}：{@code MEDIUMBLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code LONGBLOB}：{@code LONGBLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code JSON}：{@code JSON} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 * </ul>
 */
public enum StorageColumnTypeId {
    /** 整数、浮点与定点数值。 */
    TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL,
    /** 字符、二进制与时间标量。 */
    CHAR, VARCHAR, BINARY, VARBINARY, DATE, DATETIME, TIME, TIMESTAMP, YEAR,
    /** 位集合、枚举和集合。 */
    BIT, ENUM, SET, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
    /** 大对象与教学型 JSON。 */
    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON
}
