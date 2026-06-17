package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionNo：NONE 哨兵、非负校验、值往返。 */
class TransactionNoTest {

    @Test
    void noneIsZeroAndDetected() {
        assertEquals(0L, TransactionNo.NONE.value());
        assertTrue(TransactionNo.NONE.isNone());
        assertFalse(TransactionNo.of(1).isNone());
    }

    @Test
    void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> TransactionNo.of(-1));
    }

    @Test
    void valueRoundTrips() {
        assertEquals(7L, TransactionNo.of(7).value());
    }
}
