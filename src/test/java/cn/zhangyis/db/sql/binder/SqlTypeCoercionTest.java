package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.parser.SourcePosition;
import cn.zhangyis.db.sql.parser.ast.*;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 28 DD 类型的严格 literal 转换边界。 */
class SqlTypeCoercionTest {
    private static final SourcePosition P = new SourcePosition(0, 1, 1);
    private final SqlTypeCoercion coercion = new SqlTypeCoercion();

    @Test
    void supportsEveryDictionaryTypeWithoutStorageValues() {
        for (DictionaryTypeId id : DictionaryTypeId.values()) {
            ColumnTypeDefinition type = definition(id);
            SqlValue value = coercion.coerce(literal(id), type, ZoneId.of("Asia/Shanghai"), false);
            assertNotNull(value, id.name());
        }
    }

    @Test
    void preservesUnsignedBigintAndRejectsLossyOrAmbiguousValues() {
        BigInteger max = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        SqlValue.IntegerValue value = assertInstanceOf(SqlValue.IntegerValue.class,
                coercion.coerce(num(max.toString()), new ColumnTypeDefinition(DictionaryTypeId.BIGINT,
                        true, false, 0, 0, 0, 0, List.of()), ZoneId.of("UTC"), false));
        assertEquals(max, value.value());
        assertThrows(DatabaseRuntimeException.class, () -> coercion.coerce(num("18446744073709551616"),
                new ColumnTypeDefinition(DictionaryTypeId.BIGINT, true, false, 0, 0, 0, 0, List.of()),
                ZoneId.of("UTC"), false));
        assertThrows(DatabaseRuntimeException.class, () -> coercion.coerce(str("2024-11-03 01:30:00"),
                definition(DictionaryTypeId.TIMESTAMP), ZoneId.of("America/New_York"), false));
        assertThrows(DatabaseRuntimeException.class, () -> coercion.coerce(new NullLiteralNode("NULL", P),
                definition(DictionaryTypeId.INT), ZoneId.of("UTC"), true));
        SqlValue.BitValue bits = assertInstanceOf(SqlValue.BitValue.class, coercion.coerce(
                new BitLiteralNode("101", "B'101'", P),
                new ColumnTypeDefinition(DictionaryTypeId.BIT, false, false, 5, 0, 0, 0, List.of()),
                ZoneId.of("UTC"), false));
        assertArrayEquals(new byte[]{0x28}, bits.bytes(), "BIT(5) must be canonical left-aligned");
    }

    private static LiteralNode literal(DictionaryTypeId id) {
        return switch (id) {
            case TINYINT, SMALLINT, INT, BIGINT -> num("1");
            case YEAR -> num("1901");
            case FLOAT, DOUBLE -> num("1.5");
            case DECIMAL -> num("12.34");
            case CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT -> str("abc");
            case BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB -> new HexLiteralNode("ABCD", "X'ABCD'", P);
            case DATE -> str("2024-01-02");
            case DATETIME -> str("2024-01-02 03:04:05.006");
            case TIME -> str("12:34:56.007");
            case TIMESTAMP -> str("2024-01-02T03:04:05+08:00");
            case BIT -> new BitLiteralNode("101", "B'101'", P);
            case ENUM -> str("a");
            case SET -> str("a,b");
            case JSON -> str("{\"a\":1}");
        };
    }

    private static ColumnTypeDefinition definition(DictionaryTypeId id) {
        boolean nullable = false;
        return switch (id) {
            case TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, DATETIME, TIME, TIMESTAMP, YEAR ->
                    new ColumnTypeDefinition(id, false, nullable, 0, 0, 0, 0, List.of());
            case DECIMAL -> new ColumnTypeDefinition(id, false, nullable, 10, 2, 0, 0, List.of());
            case CHAR, VARCHAR, BINARY, VARBINARY, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON ->
                    new ColumnTypeDefinition(id, false, nullable, 4096, 0, 1, 1, List.of());
            case BIT -> new ColumnTypeDefinition(id, false, nullable, 8, 0, 0, 0, List.of());
            case ENUM, SET -> new ColumnTypeDefinition(id, false, nullable, 2, 0, 1, 1, List.of("a", "b"));
        };
    }

    private static NumericLiteralNode num(String value) { return new NumericLiteralNode(value, value, P); }
    private static StringLiteralNode str(String value) { return new StringLiteralNode(value, "'" + value + "'", P); }
}
