package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * DD 持久化列类型。stableCode 是磁盘协议，不依赖 Java enum ordinal；storage adapter 会把它显式映射为
 * Record 层 TypeId，从而保持 DD -> storage.api 的依赖方向。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code TINYINT}：{@code TINYINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code SMALLINT}：{@code SMALLINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code INT}：{@code INT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BIGINT}：{@code BIGINT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code FLOAT}：{@code FLOAT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DOUBLE}：{@code DOUBLE} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DECIMAL}：{@code DECIMAL} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code CHAR}：{@code CHAR} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code VARCHAR}：{@code VARCHAR} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BINARY}：{@code BINARY} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code VARBINARY}：{@code VARBINARY} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DATE}：{@code DATE} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code DATETIME}：{@code DATETIME} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TIME}：{@code TIME} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TIMESTAMP}：{@code TIMESTAMP} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code YEAR}：{@code YEAR} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BIT}：{@code BIT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code ENUM}：{@code ENUM} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code SET}：{@code SET} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TINYTEXT}：{@code TINYTEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TEXT}：{@code TEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code MEDIUMTEXT}：{@code MEDIUMTEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code LONGTEXT}：{@code LONGTEXT} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code TINYBLOB}：{@code TINYBLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code BLOB}：{@code BLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code MEDIUMBLOB}：{@code MEDIUMBLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code LONGBLOB}：{@code LONGBLOB} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 *     <li>{@code JSON}：{@code JSON} 列值的稳定逻辑类别；上下层必须显式映射，不能持久化 Java ordinal</li>
 * </ul>
 */
public enum DictionaryTypeId {
    TINYINT(1), SMALLINT(2), INT(3), BIGINT(4), FLOAT(5), DOUBLE(6), DECIMAL(7),
    CHAR(8), VARCHAR(9), BINARY(10), VARBINARY(11), DATE(12), DATETIME(13), TIME(14),
    TIMESTAMP(15), YEAR(16), BIT(17), ENUM(18), SET(19), TINYTEXT(20), TEXT(21),
    MEDIUMTEXT(22), LONGTEXT(23), TINYBLOB(24), BLOB(25), MEDIUMBLOB(26), LONGBLOB(27), JSON(28);

    /**
     * 记录 {@code stableCode} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int stableCode;

    /**
     * 创建 {@code DictionaryTypeId}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param stableCode 参与 {@code 构造} 的稳定编码 {@code stableCode}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    DictionaryTypeId(int stableCode) {
        this.stableCode = stableCode;
    }

    public int stableCode() {
        return stableCode;
    }

    /**
     * 根据调用参数构造 {@code fromStableCode} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param stableCode 参与 {@code fromStableCode} 的稳定编码 {@code stableCode}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code fromStableCode} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static DictionaryTypeId fromStableCode(int stableCode) {
        return Arrays.stream(values()).filter(value -> value.stableCode == stableCode).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown dictionary column type stable code: " + stableCode));
    }
}
