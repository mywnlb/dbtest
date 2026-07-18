package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.json.StrictJsonValidator;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.sql.binder.exception.SqlTypeCoercionException;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.parser.ast.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** DD 28 类型的 strict-mode SQL literal 转换器；不依赖 Record codec 或 storage value。 */
public final class SqlTypeCoercion {
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏SQL 名称绑定与类型推导的不变量。
     */
    private static final BigInteger TWO = BigInteger.valueOf(2);
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏SQL 名称绑定与类型推导的不变量。
     */
    private static final DateTimeFormatter LOCAL_DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd")
            .withResolverStyle(ResolverStyle.STRICT);
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏SQL 名称绑定与类型推导的不变量。
     */
    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart().appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, true).optionalEnd()
            .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏SQL 名称绑定与类型推导的不变量。
     */
    private static final Pattern SQL_TIME = Pattern.compile("([+-]?)(\\d{1,3}):(\\d{2}):(\\d{2})(?:\\.(\\d{1,3}))?");
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final Instant MIN_TIMESTAMP = Instant.parse("1970-01-01T00:00:01Z");
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final Instant MAX_TIMESTAMP = Instant.parse("2038-01-19T03:14:07.999Z");

    /** 转换 literal；primaryKey=true 时 NULL 无条件拒绝。
     *
     * @param literal 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param zoneId 参与 {@code coerce} 的稳定领域标识 {@code ZoneId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param primaryKey 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
     * @return {@code coerce} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SqlTypeCoercionException SQL 绑定、会话准入或事务结果无法按当前状态完成时抛出；调用方应报告错误并按事务边界回滚或关闭
     */
    public SqlValue coerce(LiteralNode literal, ColumnTypeDefinition type, ZoneId zoneId, boolean primaryKey) {
        if (literal == null || type == null || zoneId == null) {
            throw new DatabaseValidationException("SQL coercion literal/type/zone must not be null");
        }
        if (literal instanceof NullLiteralNode) {
            if (primaryKey || !type.nullable()) throw fail(type, "NULL is not allowed");
            return SqlValue.NullValue.INSTANCE;
        }
        try {
            return switch (type.typeId()) {
                case TINYINT -> integer(literal, type, 8);
                case SMALLINT -> integer(literal, type, 16);
                case INT -> integer(literal, type, 32);
                case BIGINT -> integer(literal, type, 64);
                case FLOAT -> floating(literal, true, type);
                case DOUBLE -> floating(literal, false, type);
                case DECIMAL -> decimal(literal, type);
                case CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT ->
                        new SqlValue.StringValue(requireString(literal, type));
                case BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB ->
                        new SqlValue.BytesValue(requireHex(literal, type));
                case DATE -> date(literal, type);
                case DATETIME -> datetime(literal, type);
                case TIME -> time(literal, type);
                case TIMESTAMP -> timestamp(literal, type, zoneId);
                case YEAR -> year(literal, type);
                case BIT -> bit(literal, type);
                case ENUM -> enumValue(literal, type);
                case SET -> setValue(literal, type);
                case JSON -> json(literal, type);
            };
        } catch (SqlTypeCoercionException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new SqlTypeCoercionException("cannot convert " + literal.lexeme() + " to " + type.typeId(), error);
        }
    }

    private static SqlValue integer(LiteralNode literal, ColumnTypeDefinition type, int bits) {
        BigInteger value = new BigInteger(requireNumeric(literal, type, true));
        BigInteger min = type.unsigned() ? BigInteger.ZERO : TWO.pow(bits - 1).negate();
        BigInteger max = type.unsigned() ? TWO.pow(bits).subtract(BigInteger.ONE)
                : TWO.pow(bits - 1).subtract(BigInteger.ONE);
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) throw fail(type, "integer is out of range");
        return new SqlValue.IntegerValue(value);
    }

    private static SqlValue floating(LiteralNode literal, boolean single, ColumnTypeDefinition type) {
        String raw = requireNumeric(literal, type, false);
        double parsed = Double.parseDouble(raw);
        double value = single ? (double) Float.parseFloat(raw) : parsed;
        if (!Double.isFinite(parsed) || !Double.isFinite(value)) throw fail(type, "floating value is not finite");
        return new SqlValue.FloatingValue(value);
    }

    private static SqlValue decimal(LiteralNode literal, ColumnTypeDefinition type) {
        BigDecimal raw = new BigDecimal(requireNumeric(literal, type, false));
        BigDecimal scaled;
        try {
            scaled = raw.setScale(type.scale(), RoundingMode.UNNECESSARY);
        } catch (ArithmeticException error) {
            throw new SqlTypeCoercionException("DECIMAL requires rounding to declared scale", error);
        }
        if (scaled.precision() > type.length()) throw fail(type, "DECIMAL precision is out of range");
        return new SqlValue.DecimalValue(scaled);
    }

    private static SqlValue date(LiteralNode literal, ColumnTypeDefinition type) {
        LocalDate date = LocalDate.parse(requireString(literal, type), LOCAL_DATE);
        if (date.isBefore(LocalDate.of(1000, 1, 1)) || date.isAfter(LocalDate.of(9999, 12, 31))) {
            throw fail(type, "DATE is outside SQL range");
        }
        return new SqlValue.TemporalValue(SqlValue.TemporalKind.DATE, date.toEpochDay());
    }

    private static SqlValue datetime(LiteralNode literal, ColumnTypeDefinition type) {
        LocalDateTime local = LocalDateTime.parse(requireString(literal, type), LOCAL_DATE_TIME);
        if (local.toLocalDate().isBefore(LocalDate.of(1000, 1, 1))) throw fail(type, "DATETIME is outside SQL range");
        return new SqlValue.TemporalValue(SqlValue.TemporalKind.DATETIME,
                local.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    private static SqlValue time(LiteralNode literal, ColumnTypeDefinition type) {
        String raw = requireString(literal, type);
        Matcher matcher = SQL_TIME.matcher(raw);
        if (!matcher.matches()) throw fail(type, "TIME has invalid syntax");
        int hour = Integer.parseInt(matcher.group(2));
        int minute = Integer.parseInt(matcher.group(3));
        int second = Integer.parseInt(matcher.group(4));
        int millis = fractionMillis(matcher.group(5));
        if (hour > 838 || minute > 59 || second > 59) throw fail(type, "TIME is outside SQL range");
        long total = (((long) hour * 60 + minute) * 60 + second) * 1000 + millis;
        if (matcher.group(1).equals("-")) total = -total;
        return new SqlValue.TemporalValue(SqlValue.TemporalKind.TIME, total);
    }

    private static SqlValue timestamp(LiteralNode literal, ColumnTypeDefinition type, ZoneId zoneId) {
        String raw = requireString(literal, type);
        Instant instant;
        try {
            instant = OffsetDateTime.parse(raw, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        } catch (RuntimeException noOffset) {
            LocalDateTime local = LocalDateTime.parse(raw, LOCAL_DATE_TIME);
            List<ZoneOffset> offsets = zoneId.getRules().getValidOffsets(local);
            if (offsets.size() != 1) throw fail(type, "TIMESTAMP local time is DST gap/ambiguous; offset required");
            instant = local.toInstant(offsets.getFirst());
        }
        if (instant.isBefore(MIN_TIMESTAMP) || instant.isAfter(MAX_TIMESTAMP)) {
            throw fail(type, "TIMESTAMP is outside SQL range");
        }
        return new SqlValue.TemporalValue(SqlValue.TemporalKind.TIMESTAMP, instant.toEpochMilli());
    }

    private static SqlValue year(LiteralNode literal, ColumnTypeDefinition type) {
        String raw = literal instanceof StringLiteralNode string ? string.value() : requireNumeric(literal, type, true);
        int year = Integer.parseInt(raw);
        if (year != 0 && (year < 1901 || year > 2155)) throw fail(type, "YEAR is outside SQL range");
        return new SqlValue.TemporalValue(SqlValue.TemporalKind.YEAR, year);
    }

    private static SqlValue bit(LiteralNode literal, ColumnTypeDefinition type) {
        if (!(literal instanceof BitLiteralNode bits)) throw fail(type, "BIT requires B'...' literal");
        if (bits.value().isEmpty() || bits.value().length() > type.length()) throw fail(type, "BIT width overflow");
        String canonical = "0".repeat(type.length() - bits.value().length()) + bits.value();
        byte[] bytes = new byte[(type.length() + 7) / 8];
        for (int i = 0; i < canonical.length(); i++) {
            if (canonical.charAt(i) == '1') bytes[i / 8] |= (byte) (1 << (7 - i % 8));
        }
        return new SqlValue.BitValue(bytes, type.length());
    }

    private static SqlValue enumValue(LiteralNode literal, ColumnTypeDefinition type) {
        String value = requireString(literal, type);
        int index = type.symbols().indexOf(value);
        if (index < 0) throw fail(type, "unknown ENUM symbol");
        return new SqlValue.EnumValue(value, index + 1);
    }

    private static SqlValue setValue(LiteralNode literal, ColumnTypeDefinition type) {
        String raw = requireString(literal, type);
        if (raw.isEmpty()) return new SqlValue.SetValue(List.of(), 0L);
        String[] requested = raw.split(",", -1);
        HashSet<String> unique = new HashSet<>();
        long bitmap = 0L;
        for (String symbol : requested) {
            int index = type.symbols().indexOf(symbol);
            if (index < 0 || !unique.add(symbol)) throw fail(type, "unknown/duplicate SET symbol");
            bitmap |= 1L << index;
        }
        List<String> ordered = new ArrayList<>();
        for (String symbol : type.symbols()) if (unique.contains(symbol)) ordered.add(symbol);
        return new SqlValue.SetValue(ordered, bitmap);
    }

    private static SqlValue json(LiteralNode literal, ColumnTypeDefinition type) {
        String value = requireString(literal, type);
        try {
            StrictJsonValidator.validate(value);
        } catch (RuntimeException error) {
            throw new SqlTypeCoercionException("invalid JSON literal", error);
        }
        return new SqlValue.StringValue(value);
    }

    private static String requireNumeric(LiteralNode literal, ColumnTypeDefinition type, boolean integerOnly) {
        if (!(literal instanceof NumericLiteralNode numeric)) throw fail(type, "numeric literal required");
        String value = numeric.value();
        if (integerOnly && (value.indexOf('.') >= 0 || value.indexOf('e') >= 0 || value.indexOf('E') >= 0)) {
            throw fail(type, "integer literal required");
        }
        return value;
    }

    private static String requireString(LiteralNode literal, ColumnTypeDefinition type) {
        if (!(literal instanceof StringLiteralNode string)) throw fail(type, "string literal required");
        return string.value();
    }

    private static byte[] requireHex(LiteralNode literal, ColumnTypeDefinition type) {
        if (!(literal instanceof HexLiteralNode hex)) throw fail(type, "hex literal required");
        byte[] result = new byte[hex.value().length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.value().substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static int fractionMillis(String value) {
        if (value == null) return 0;
        return Integer.parseInt(value + "0".repeat(3 - value.length()));
    }

    private static SqlTypeCoercionException fail(ColumnTypeDefinition type, String message) {
        return new SqlTypeCoercionException(type.typeId() + ": " + message);
    }
}
