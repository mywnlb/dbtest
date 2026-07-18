package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.text.Normalizer;
import java.util.Locale;
import java.util.Objects;

/**
 * Data Dictionary 对象名。displayName 保留用户输入用于诊断，canonicalName 是名称唯一性、MDL key 和
 * catalog name directory 的唯一权威。v1 固定使用 NFC + Locale.ROOT 小写，等价于教学版始终开启
 * 大小写不敏感标识符，不尝试模拟平台相关的 lower_case_table_names。
 */
public final class ObjectName implements Comparable<ObjectName> {

    /** MySQL 普通 schema/table/column/index 标识符上限。 */
    public static final int MAX_CODE_POINTS = 64;

    /** 用户输入的展示形式，不参与相等性。 */
    private final String displayName;

    /** NFC 且 Locale.ROOT 小写后的名称身份。 */
    private final String canonicalName;

    private ObjectName(String displayName, String canonicalName) {
        this.displayName = displayName;
        this.canonicalName = canonicalName;
    }

    /**
     * 创建并规范化一个字典对象名。校验必须早于文件、MDL 或 catalog 操作，避免同一非法名称留下部分状态。
     *
     * @param value 用户提供的对象名。
     * @return 具有稳定 canonical identity 的对象名。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static ObjectName of(String value) {
        if (value == null || value.isBlank()) {
            throw new DatabaseValidationException("dictionary object name must not be blank");
        }
        if (value.codePointCount(0, value.length()) > MAX_CODE_POINTS) {
            throw new DatabaseValidationException("dictionary object name exceeds 64 code points: " + value);
        }
        value.codePoints().forEach(codePoint -> {
            if (Character.isISOControl(codePoint)) {
                throw new DatabaseValidationException(
                        "dictionary object name must not contain control characters");
            }
        });
        String canonical = Normalizer.normalize(value, Normalizer.Form.NFC).toLowerCase(Locale.ROOT);
        return new ObjectName(value, canonical);
    }

    public String displayName() {
        return displayName;
    }

    public String canonicalName() {
        return canonicalName;
    }

    /**
     * 实现 {@code compareTo} 的稳定值语义；比较只读取输入与本对象，不改变数据字典状态。
     *
     * @param other 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public int compareTo(ObjectName other) {
        if (other == null) {
            throw new DatabaseValidationException("dictionary object name comparison target must not be null");
        }
        return canonicalName.compareTo(other.canonicalName);
    }

    /**
     * 实现 {@code equals} 的稳定值语义；比较只读取输入与本对象，不改变数据字典状态。
     *
     * @param other 待比较对象；允许为 {@code null} 或其他类型，此时按 {@code equals} 契约返回 {@code false}
     * @return 比较对象类型与全部值语义相等时为 {@code true}；对象为 {@code null}、类型不同或任一组件不等时为 {@code false}
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ObjectName that
                && canonicalName.equals(that.canonicalName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canonicalName);
    }

    @Override
    public String toString() {
        return displayName;
    }
}
