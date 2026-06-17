package cn.zhangyis.db.storage.trx;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionState 状态机：合法 commit/rollback 路径与非法转换。 */
class TransactionStateTest {

    @Test
    void legalCommitPath() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.COMMITTING));
        assertTrue(TransactionState.COMMITTING.canTransitionTo(TransactionState.COMMITTED));
    }

    @Test
    void legalRollbackPath() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.ROLLING_BACK));
        assertTrue(TransactionState.ROLLING_BACK.canTransitionTo(TransactionState.ROLLED_BACK));
    }

    @Test
    void illegalTransitionsRejected() {
        assertFalse(TransactionState.COMMITTED.canTransitionTo(TransactionState.ACTIVE));
        assertFalse(TransactionState.ACTIVE.canTransitionTo(TransactionState.COMMITTED));
        assertFalse(TransactionState.ROLLED_BACK.canTransitionTo(TransactionState.COMMITTING));
        assertFalse(TransactionState.COMMITTING.canTransitionTo(TransactionState.ROLLING_BACK));
    }
}
