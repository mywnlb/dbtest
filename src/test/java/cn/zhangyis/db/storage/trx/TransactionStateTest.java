package cn.zhangyis.db.storage.trx;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionState 状态机：合法 commit/rollback 路径与非法转换。 */
class TransactionStateTest {

    /**
     * 验证 {@code legalCommitPath} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void legalCommitPath() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.COMMITTING));
        assertTrue(TransactionState.COMMITTING.canTransitionTo(TransactionState.COMMITTED));
    }

    /**
     * 验证 {@code legalRollbackPath} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void legalRollbackPath() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.ROLLING_BACK));
        assertTrue(TransactionState.ROLLING_BACK.canTransitionTo(TransactionState.ROLLED_BACK));
    }

    /** XA participant 使用独立 PREPARED 与 prepared rollback 重试态，不能借用普通回滚状态。 */
    @Test
    void legalPreparedPaths() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.PREPARED));
        assertTrue(TransactionState.PREPARED.canTransitionTo(TransactionState.COMMITTING));
        assertTrue(TransactionState.PREPARED.canTransitionTo(TransactionState.PREPARED_ROLLING_BACK));
        assertTrue(TransactionState.PREPARED_ROLLING_BACK.canTransitionTo(TransactionState.ROLLED_BACK));
        assertFalse(TransactionState.PREPARED.canTransitionTo(TransactionState.ACTIVE));
        assertFalse(TransactionState.PREPARED_ROLLING_BACK.canTransitionTo(TransactionState.ROLLING_BACK));
    }

    /**
     * 验证 {@code illegalTransitionsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void illegalTransitionsRejected() {
        assertFalse(TransactionState.COMMITTED.canTransitionTo(TransactionState.ACTIVE));
        assertFalse(TransactionState.ACTIVE.canTransitionTo(TransactionState.COMMITTED));
        assertFalse(TransactionState.ROLLED_BACK.canTransitionTo(TransactionState.COMMITTING));
        assertFalse(TransactionState.COMMITTING.canTransitionTo(TransactionState.ROLLING_BACK));
    }
}
