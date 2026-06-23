package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UndoNoTest {
    @Test void noneIsZeroAndDetected() {
        assertEquals(0L, UndoNo.NONE.value());
        assertTrue(UndoNo.NONE.isNone());
        assertFalse(UndoNo.of(1).isNone());
    }
    @Test void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> UndoNo.of(-1));
    }
    @Test void valueRoundTrips() {
        assertEquals(42L, UndoNo.of(42).value());
    }
}
