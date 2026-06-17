package cn.zhangyis.db.storage.record.schema;

/** 第一片支持的列类型（innodb-record-design §8.2 子集）。 */
public enum TypeId {
    TINYINT, SMALLINT, INT, BIGINT,
    FLOAT, DOUBLE, DECIMAL,
    CHAR, VARCHAR, BINARY, VARBINARY,
    DATE, DATETIME
}
