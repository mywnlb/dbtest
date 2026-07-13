package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ColumnType 工厂与校验：storageKind 一致、DECIMAL/CHAR 参数边界。 */
class ColumnTypeTest {

    @Test
    void integerFactories() {
        ColumnType t = ColumnType.intType(false, true);
        assertEquals(TypeId.INT, t.typeId());
        assertTrue(t.nullable());
        assertFalse(t.unsigned());
        assertEquals(StorageKind.FIXED, t.storageKind());
    }

    @Test
    void varcharIsVariableCharIsFixed() {
        assertEquals(StorageKind.VARIABLE, ColumnType.varchar(20, false).storageKind());
        assertEquals(StorageKind.FIXED, ColumnType.charType(10, false).storageKind());
        assertEquals(StorageKind.VARIABLE, ColumnType.varbinary(8, true).storageKind());
        assertEquals(StorageKind.FIXED, ColumnType.binary(8, true).storageKind());
    }

    /** TEXT/BLOB/JSON 进入变长目录，但额外声明允许 external reference。 */
    @Test
    void lobFactoriesExposeStableByteLimitsAndOverflowStorage() {
        assertEquals(255, ColumnType.tinyText(false).length());
        assertEquals(65_535, ColumnType.text(false).length());
        assertEquals(16_777_215, ColumnType.mediumBlob(false).length());
        assertEquals(Integer.MAX_VALUE, ColumnType.longBlob(false).length());
        assertEquals(StorageKind.OVERFLOW_CAPABLE, ColumnType.longText(false).storageKind());
        assertEquals(StorageKind.OVERFLOW_CAPABLE, ColumnType.json(false).storageKind());
        assertEquals(CharsetId.UTF8, ColumnType.json(false).charset());
    }

    @Test
    void characterFactoriesKeepBinaryDefaultAndAcceptExplicitPair() {
        ColumnType defaultType = ColumnType.varchar(20, false);
        assertEquals(CharsetId.UTF8, defaultType.charset());
        assertEquals(CollationId.BINARY, defaultType.collation());

        ColumnType latin1 = ColumnType.charType(
                10, true, CharsetId.LATIN1, CollationId.LATIN1_ASCII_CI);
        assertEquals(CharsetId.LATIN1, latin1.charset());
        assertEquals(CollationId.LATIN1_ASCII_CI, latin1.collation());
    }

    @Test
    void decimalValidatesPrecisionScale() {
        ColumnType d = ColumnType.decimal(10, 2, false);
        assertEquals(10, d.length());
        assertEquals(2, d.scale());
        assertThrows(DatabaseValidationException.class, () -> ColumnType.decimal(0, 0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.decimal(39, 0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.decimal(5, 6, false));
    }

    @Test
    void charLengthMustBePositive() {
        assertThrows(DatabaseValidationException.class, () -> ColumnType.charType(0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.varchar(-1, false));
    }

    @Test
    void temporalAndFloating() {
        assertEquals(TypeId.DATE, ColumnType.date(true).typeId());
        assertEquals(TypeId.TIME, ColumnType.time(false).typeId());
        assertEquals(TypeId.DATETIME, ColumnType.datetime(false).typeId());
        assertEquals(TypeId.TIMESTAMP, ColumnType.timestamp(true).typeId());
        assertEquals(TypeId.YEAR, ColumnType.year(false).typeId());
        assertEquals(StorageKind.FIXED, ColumnType.time(false).storageKind());
        assertEquals(StorageKind.FIXED, ColumnType.timestamp(false).storageKind());
        assertEquals(StorageKind.FIXED, ColumnType.year(false).storageKind());
        assertEquals(TypeId.FLOAT, ColumnType.floatType(false).typeId());
        assertEquals(TypeId.DOUBLE, ColumnType.doubleType(true).typeId());
    }

    /** 时间标量的宽度与 signedness 由 TypeId 固定，schema 不能携带冲突元数据。 */
    @Test
    void temporalMetadataMustStayCanonical() {
        assertThrows(DatabaseValidationException.class,
                () -> new ColumnType(TypeId.TIME, false, 8, 0, false,
                        CharsetId.UTF8, CollationId.BINARY, StorageKind.FIXED, List.of()));
        assertThrows(DatabaseValidationException.class,
                () -> new ColumnType(TypeId.YEAR, false, 0, 0, true,
                        CharsetId.UTF8, CollationId.BINARY, StorageKind.FIXED, List.of()));
    }

    /** BIT(n) 的 length 是 bit width，首片只接受 1..64 且固定存储。 */
    @Test
    void bitWidthAndStorageAreValidated() {
        for (int width : new int[] {1, 8, 9, 64}) {
            ColumnType type = ColumnType.bit(width, false);
            assertEquals(TypeId.BIT, type.typeId());
            assertEquals(width, type.length());
            assertEquals(StorageKind.FIXED, type.storageKind());
        }
        assertThrows(DatabaseValidationException.class, () -> ColumnType.bit(0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.bit(65, false));
    }

    /** ENUM/SET 字典属于不可变 schema，声明顺序决定 ordinal/bitmap。 */
    @Test
    void enumeratedDictionariesAreImmutableAndValidated() {
        List<String> source = new java.util.ArrayList<>(List.of("NEW", "DONE"));
        ColumnType enumType = ColumnType.enumType(source, false);
        source.add("BROKEN");
        assertEquals(TypeId.ENUM, enumType.typeId());
        assertEquals(List.of("NEW", "DONE"), enumType.symbols());
        assertEquals(StorageKind.FIXED, enumType.storageKind());

        ColumnType setType = ColumnType.setType(List.of("READ", "WRITE", "ADMIN"), true);
        assertEquals(TypeId.SET, setType.typeId());
        assertEquals(3, setType.length());
        assertThrows(UnsupportedOperationException.class, () -> setType.symbols().add("ROOT"));
        assertThrows(DatabaseValidationException.class,
                () -> ColumnType.enumType(List.of("A", "A"), false));
        assertThrows(DatabaseValidationException.class,
                () -> ColumnType.setType(List.of("A", " "), false));
        assertThrows(DatabaseValidationException.class,
                () -> ColumnType.setType(java.util.Collections.nCopies(65, "x"), false));
    }
}
