package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionOptions：默认值与 null 校验。 */
class TransactionOptionsTest {

    @Test
    void defaultsAreRepeatableReadReadWriteAutocommit() {
        TransactionOptions o = TransactionOptions.defaults();
        assertEquals(IsolationLevel.REPEATABLE_READ, o.isolationLevel());
        assertFalse(o.readOnly());
        assertTrue(o.autoCommit());
    }

    @Test
    void rejectsNullIsolationLevel() {
        assertThrows(DatabaseValidationException.class,
                () -> new TransactionOptions(null, false, true));
    }
}
