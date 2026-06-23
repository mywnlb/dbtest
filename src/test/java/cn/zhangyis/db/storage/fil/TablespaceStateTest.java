package cn.zhangyis.db.storage.fil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    /**
     * page-0 会持久化显式状态码；本测试固定编码协议，防止后续调整枚举声明顺序破坏磁盘兼容性。
     */
    @Test
    void shouldRoundTripStablePersistentCodes() {
        assertEquals(0, TablespaceState.EMPTY.persistentCode());
        assertEquals(1, TablespaceState.NORMAL.persistentCode());
        assertEquals(2, TablespaceState.ACTIVE.persistentCode());
        assertEquals(3, TablespaceState.INACTIVE.persistentCode());
        assertEquals(4, TablespaceState.TRUNCATING.persistentCode());
        assertEquals(5, TablespaceState.DISCARDED.persistentCode());
        assertEquals(6, TablespaceState.CORRUPTED.persistentCode());

        for (TablespaceState state : TablespaceState.values()) {
            assertEquals(state, TablespaceState.fromPersistentCode(state.persistentCode()));
        }
        assertThrows(RuntimeException.class, () -> TablespaceState.fromPersistentCode(99));
    }
}
