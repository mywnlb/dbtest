package cn.zhangyis.db.storage.api.ddl;

/** storage.api 对上暴露的稳定列类型集合；不泄漏 record 包的物理 codec 类型。 */
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
