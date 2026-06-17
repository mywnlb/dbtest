package cn.zhangyis.db.storage.fil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TablespaceState 测试固定表空间生命周期状态机，避免 DDL、恢复和普通 IO 后续各自随意改状态。
 */
class TablespaceStateTest {

    @Test
    void shouldAllowNormalLifecycleTransitions() {
        assertTrue(TablespaceState.EMPTY.canTransitTo(TablespaceState.NORMAL));
        assertTrue(TablespaceState.NORMAL.canTransitTo(TablespaceState.ACTIVE));
        assertTrue(TablespaceState.ACTIVE.canTransitTo(TablespaceState.INACTIVE));
        assertTrue(TablespaceState.INACTIVE.canTransitTo(TablespaceState.ACTIVE));
        assertTrue(TablespaceState.INACTIVE.canTransitTo(TablespaceState.DISCARDED));
        assertTrue(TablespaceState.ACTIVE.canTransitTo(TablespaceState.CORRUPTED));
    }

    @Test
    void shouldRejectUnsafeLifecycleTransitions() {
        assertFalse(TablespaceState.CORRUPTED.canTransitTo(TablespaceState.ACTIVE));
        assertFalse(TablespaceState.DISCARDED.canTransitTo(TablespaceState.ACTIVE));
        assertFalse(TablespaceState.NORMAL.canTransitTo(TablespaceState.EMPTY));
    }
}
