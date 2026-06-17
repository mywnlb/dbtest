package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * ExtentAllocationPolicy 值表与 NoFreeSpaceException 可恢复分类。
 */
class ExtentAllocationPolicyTest {

    @Test
    void defaultPolicyClampsToFour() {
        ExtentAllocationPolicy p = new DefaultExtentAllocationPolicy();
        assertEquals(1, p.extentsToAcquire(0));
        assertEquals(1, p.extentsToAcquire(1));
        assertEquals(2, p.extentsToAcquire(2));
        assertEquals(3, p.extentsToAcquire(3));
        assertEquals(4, p.extentsToAcquire(4));
        assertEquals(4, p.extentsToAcquire(10));
    }

    @Test
    void noFreeSpaceIsRecoverable() {
        assertInstanceOf(DatabaseRuntimeException.class, new NoFreeSpaceException("x"));
    }
}
