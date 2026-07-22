package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** DDL schema canonical image 版本；版本只增不改，已有版本的任一字节规则均不可重定义。 */
public enum DdlSchemaCanonicalFormat {
    /** 表聚合、schema identity、row format 与 LOB capability 的第一版稳定编码。 */
    TABLE_SCHEMA_V1(1);

    /** marker 中持久化且参与摘要输入的稳定正编码。 */
    private final int stableCode;

    DdlSchemaCanonicalFormat(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return marker 与 canonical image 共用的稳定正编码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码 marker 中的 canonical format 稳定码。
     *
     * @param code v4 payload 中读取的无符号稳定码
     * @return 与稳定码唯一对应的 canonical format
     * @throws DatabaseValidationException code 未声明时抛出，调用方不得套用其它格式
     */
    public static DdlSchemaCanonicalFormat fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown DDL schema canonical format stable code: " + code));
    }
}
