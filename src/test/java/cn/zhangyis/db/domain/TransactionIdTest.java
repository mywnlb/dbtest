package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionId：NONE 哨兵、非负校验、值往返。 */
class TransactionIdTest {

    @Test
    void noneIsZeroAndDetected() {
        assertEquals(0L, TransactionId.NONE.value());
        assertTrue(TransactionId.NONE.isNone());
        assertFalse(TransactionId.of(1).isNone());
    }

    @Test
    void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> TransactionId.of(-1));
    }

    @Test
    void valueRoundTrips() {
        assertEquals(42L, TransactionId.of(42).value());
    }
}
