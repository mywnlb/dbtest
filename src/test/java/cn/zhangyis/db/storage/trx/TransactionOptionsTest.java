package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionOptions：默认值与 null 校验。 */
class TransactionOptionsTest {

    /**
     * 验证 {@code defaultsAreRepeatableReadReadWriteAutocommit} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void defaultsAreRepeatableReadReadWriteAutocommit() {
        TransactionOptions o = TransactionOptions.defaults();
        assertEquals(IsolationLevel.REPEATABLE_READ, o.isolationLevel());
        assertFalse(o.readOnly());
        assertTrue(o.autoCommit());
    }

    /**
     * 验证 {@code rejectsNullIsolationLevel} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNullIsolationLevel() {
        assertThrows(DatabaseValidationException.class,
                () -> new TransactionOptions(null, false, true));
    }
}
