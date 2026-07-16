package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * DD 持久化列类型。stableCode 是磁盘协议，不依赖 Java enum ordinal；storage adapter 会把它显式映射为
 * Record 层 TypeId，从而保持 DD -> storage.api 的依赖方向。
 */
public enum DictionaryTypeId {
    TINYINT(1), SMALLINT(2), INT(3), BIGINT(4), FLOAT(5), DOUBLE(6), DECIMAL(7),
    CHAR(8), VARCHAR(9), BINARY(10), VARBINARY(11), DATE(12), DATETIME(13), TIME(14),
    TIMESTAMP(15), YEAR(16), BIT(17), ENUM(18), SET(19), TINYTEXT(20), TEXT(21),
    MEDIUMTEXT(22), LONGTEXT(23), TINYBLOB(24), BLOB(25), MEDIUMBLOB(26), LONGBLOB(27), JSON(28);

    private final int stableCode;

    DictionaryTypeId(int stableCode) {
        this.stableCode = stableCode;
    }

    public int stableCode() {
        return stableCode;
    }

    public static DictionaryTypeId fromStableCode(int stableCode) {
        return Arrays.stream(values()).filter(value -> value.stableCode == stableCode).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown dictionary column type stable code: " + stableCode));
    }
}
