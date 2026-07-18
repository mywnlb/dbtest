package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/** 完整用户 SQL 值；external LOB 在进入此层前必须已 hydrate。 */
public sealed interface SqlValue permits SqlValue.NullValue, SqlValue.IntegerValue,
        SqlValue.FloatingValue, SqlValue.DecimalValue, SqlValue.StringValue, SqlValue.BytesValue,
        SqlValue.TemporalValue, SqlValue.BitValue, SqlValue.EnumValue, SqlValue.SetValue {

    /** SQL NULL 单例。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code INSTANCE}：无可变状态的单例值，避免为相同空值语义重复分配对象</li>
     * </ul>
     */
    enum NullValue implements SqlValue { INSTANCE }

    /** 覆盖 signed/unsigned BIGINT 全域的整数投影。
     *
     * @param value 待映射为 SQL 精确数值的任意精度值；不得为 {@code null}，精度和标度必须落在目标列类型允许范围
     */
    record IntegerValue(BigInteger value) implements SqlValue {
        public IntegerValue {
            if (value == null) throw new DatabaseValidationException("SQL integer must not be null");
        }
    }

    /** FLOAT/DOUBLE 的有限 IEEE 值。
     *
     * @param value 由 {@code 构造} 转换或编码的原始 {@code double} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    record FloatingValue(double value) implements SqlValue {
        public FloatingValue {
            if (!Double.isFinite(value)) throw new DatabaseValidationException("SQL floating value must be finite");
        }
    }

    /** 精确 DECIMAL 投影。
     *
     * @param value 待映射为 SQL 精确数值的任意精度值；不得为 {@code null}，精度和标度必须落在目标列类型允许范围
     */
    record DecimalValue(BigDecimal value) implements SqlValue {
        public DecimalValue {
            if (value == null) throw new DatabaseValidationException("SQL decimal must not be null");
        }
    }

    /** 字符、ENUM/JSON 之外的普通字符串投影。
     *
     * @param value 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    record StringValue(String value) implements SqlValue {
        public StringValue {
            if (value == null) throw new DatabaseValidationException("SQL string must not be null");
        }
    }

    /** BINARY/BLOB 完整字节值；构造和访问均防御性复制。 */
    final class BytesValue implements SqlValue {
        /**
         * 本对象独占的 {@code value} 数据缓冲；构造和访问路径必须遵守防御性复制或受控视图约束，不能泄漏可变数组。
         */
        private final byte[] value;
        /**
         * 创建 {@code BytesValue}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param value 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        public BytesValue(byte[] value) {
            if (value == null) throw new DatabaseValidationException("SQL bytes must not be null");
            this.value = Arrays.copyOf(value, value.length);
        }
        public byte[] value() { return Arrays.copyOf(value, value.length); }
        /**
         * 实现 {@code equals} 的稳定值语义；比较只读取输入与本对象，不改变SQL 计划执行状态。
         *
         * @param other 待比较对象；允许为 {@code null} 或其他类型，此时按 {@code equals} 契约返回 {@code false}
         * @return 比较对象类型与全部值语义相等时为 {@code true}；对象为 {@code null}、类型不同或任一组件不等时为 {@code false}
         */
        @Override public boolean equals(Object other) {
            return other instanceof BytesValue bytes && Arrays.equals(value, bytes.value);
        }
        @Override public int hashCode() { return Arrays.hashCode(value); }
    }

    /** DATE/TIME/DATETIME/TIMESTAMP/YEAR 的稳定公开投影。
     *
     * @param kind 选择 {@code 构造} 分支的 {@code TemporalKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param value 由 {@code 构造} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    record TemporalValue(TemporalKind kind, long value) implements SqlValue {
        public TemporalValue {
            if (kind == null) throw new DatabaseValidationException("SQL temporal kind must not be null");
        }
    }

    /**
     * 定义SQL 计划执行的 {@code TemporalKind} 状态或类别；枚举值用于显式分派领域行为，不得用声明顺序代替稳定编码。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code DATE}：表示“DATE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code TIME}：表示“TIME”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code DATETIME}：表示“DATETIME”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code TIMESTAMP}：表示“TIMESTAMP”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code YEAR}：表示“YEAR”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     * </ul>
     */
    enum TemporalKind { DATE, TIME, DATETIME, TIMESTAMP, YEAR }

    /** canonical 左补零 bit string。 */
    final class BitValue implements SqlValue {
        /**
         * 本对象独占的 {@code bytes} 数据缓冲；构造和访问路径必须遵守防御性复制或受控视图约束，不能泄漏可变数组。
         */
        private final byte[] bytes;
        /**
         * 记录 {@code bitWidth} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
         */
        private final int bitWidth;
        /**
         * 创建 {@code BitValue}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
         * @param bitWidth 参与 {@code 构造} 的原始数值身份 {@code bitWidth}；必须非负，零值仅用于对应格式明确声明的系统或空身份
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        public BitValue(byte[] bytes, int bitWidth) {
            if (bytes == null || bitWidth <= 0 || bytes.length != (bitWidth + 7) / 8) {
                throw new DatabaseValidationException("invalid SQL bit value framing");
            }
            this.bytes = Arrays.copyOf(bytes, bytes.length);
            this.bitWidth = bitWidth;
        }
        public byte[] bytes() { return Arrays.copyOf(bytes, bytes.length); }
        public int bitWidth() { return bitWidth; }
        /**
         * 实现 {@code equals} 的稳定值语义；比较只读取输入与本对象，不改变SQL 计划执行状态。
         *
         * @param other 待比较对象；允许为 {@code null} 或其他类型，此时按 {@code equals} 契约返回 {@code false}
         * @return 比较对象类型与全部值语义相等时为 {@code true}；对象为 {@code null}、类型不同或任一组件不等时为 {@code false}
         */
        @Override public boolean equals(Object other) {
            return other instanceof BitValue bit && bitWidth == bit.bitWidth && Arrays.equals(bytes, bit.bytes);
        }
        @Override public int hashCode() { return 31 * bitWidth + Arrays.hashCode(bytes); }
    }

    /** ENUM 声明 symbol 与 1-based ordinal。
     *
     * @param symbol 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     */
    record EnumValue(String symbol, int ordinal) implements SqlValue {
        public EnumValue {
            if (symbol == null || ordinal <= 0) throw new DatabaseValidationException("invalid SQL ENUM value");
        }
    }

    /** SET 的声明顺序 symbol 列表与 bitmap。
     *
     * @param symbols 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param bitmap 参与 {@code 构造} 的无符号位模式 {@code bitmap}；保留全部原始位，不能按 Java 有符号数值语义截断或重排
     */
    record SetValue(List<String> symbols, long bitmap) implements SqlValue {
        public SetValue {
            if (symbols == null || symbols.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException("invalid SQL SET value");
            }
            symbols = List.copyOf(symbols);
        }
    }
}
