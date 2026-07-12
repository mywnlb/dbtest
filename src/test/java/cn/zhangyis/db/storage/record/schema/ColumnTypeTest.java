package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

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
        assertEquals(TypeId.DATETIME, ColumnType.datetime(false).typeId());
        assertEquals(TypeId.FLOAT, ColumnType.floatType(false).typeId());
        assertEquals(TypeId.DOUBLE, ColumnType.doubleType(true).typeId());
    }
}
