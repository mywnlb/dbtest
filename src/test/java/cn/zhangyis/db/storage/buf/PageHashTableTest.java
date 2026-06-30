package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.10d PageHashTable 单元测试：PageId→BufferFrame 映射的 put/get/remove/containsKey/size，以及 countInRange 区间计数。
 * 本类不自带锁，由所属 BufferPoolInstance 的 instanceLock 在外保护；测试单线程直接调用。
 */
class PageHashTableTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    private static BufferFrame frame() {
        return new BufferFrame(PS);
    }

    @Test
    void putGetRemoveContainsSize() {
        PageHashTable table = new PageHashTable();
        BufferFrame f = frame();

        assertNull(table.get(page(2)), "未放入前 get 返回 null");
        assertFalse(table.containsKey(page(2)));
        assertEquals(0, table.size());

        table.put(page(2), f);
        assertSame(f, table.get(page(2)), "get 返回放入的同一帧");
        assertTrue(table.containsKey(page(2)));
        assertEquals(1, table.size());

        BufferFrame removed = table.remove(page(2));
        assertSame(f, removed, "remove 返回被移除的帧");
        assertFalse(table.containsKey(page(2)));
        assertEquals(0, table.size());
    }

    @Test
    void countInRangeCountsOnlyPresentKeys() {
        PageHashTable table = new PageHashTable();
        for (long no : new long[]{2, 4, 5, 8}) {
            table.put(page(no), frame());
        }
        // 区间 [2,6) = 页 2,3,4,5；存在 2,4,5 → 计 3（缺 3 不计、区间外 8 不计）。
        assertEquals(3, table.countInRange(SPACE, 2, 4));
        assertEquals(0, table.countInRange(SPACE, 10, 4), "区间内无键计 0");
        assertEquals(0, table.countInRange(SpaceId.of(99), 2, 4), "其它表空间同区间计 0");
        assertEquals(1, table.countInRange(SPACE, 8, 1), "单页区间命中计 1");
    }

    @Test
    void valuesAndKeysSnapshot() {
        PageHashTable table = new PageHashTable();
        BufferFrame a = frame();
        BufferFrame b = frame();
        table.put(page(1), a);
        table.put(page(2), b);
        assertEquals(2, table.values().size());
        assertTrue(table.values().contains(a) && table.values().contains(b));
        assertTrue(table.keySet().contains(page(1)) && table.keySet().contains(page(2)));
    }

    @Test
    void countInRangeRejectsInvalidArguments() {
        PageHashTable table = new PageHashTable();
        assertThrows(DatabaseValidationException.class, () -> table.countInRange(null, 0, 1));
        assertThrows(DatabaseValidationException.class, () -> table.countInRange(SPACE, -1, 1));
        assertThrows(DatabaseValidationException.class, () -> table.countInRange(SPACE, 0, 0));
    }
}
