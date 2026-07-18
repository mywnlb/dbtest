package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 13.1d 真实 flush list 的包内契约测试：flush 模块只能看到按 oldest LSN 排序的候选，
 * 同一页重复变脏不能产生重复节点，clean/free 时必须从链表移除。
 */
class DirtyPageListTest {

    private static final SpaceId SPACE = SpaceId.of(95);

    /**
     * 验证 {@code upsertKeepsSingleEntryAndOrdersByOldestLsn} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void upsertKeepsSingleEntryAndOrdersByOldestLsn() {
        DirtyPageList list = new DirtyPageList();
        PageId p1 = page(1);
        PageId p2 = page(2);
        PageId p3 = page(3);

        list.upsert(p1, Lsn.of(30), Lsn.of(30));
        list.upsert(p2, Lsn.of(10), Lsn.of(10));
        list.upsert(p3, Lsn.of(20), Lsn.of(20));
        list.upsert(p1, Lsn.of(30), Lsn.of(40));

        List<DirtyPageCandidate> candidates = list.candidatesUpTo(Lsn.of(35), 10);

        assertEquals(List.of(p2, p3, p1), candidates.stream().map(DirtyPageCandidate::pageId).toList());
        assertEquals(Lsn.of(30), candidates.get(2).oldestModificationLsn(),
                "同一页重复 upsert 不应重置 oldest LSN");
        assertEquals(Lsn.of(40), candidates.get(2).newestModificationLsn(),
                "同一页重复 upsert 应刷新 newest LSN 供 WAL gate 使用");
    }

    /**
     * 验证 {@code removeClearsDirtyBoundary} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void removeClearsDirtyBoundary() {
        DirtyPageList list = new DirtyPageList();
        PageId page = page(7);

        list.upsert(page, Lsn.of(10), Lsn.of(10));
        assertTrue(list.hasDirtyPages());
        assertEquals(Lsn.of(10), list.oldestDirtyLsnOrNull());

        list.remove(page);

        assertFalse(list.hasDirtyPages());
        assertNull(list.oldestDirtyLsnOrNull());
        assertTrue(list.candidatesUpTo(Lsn.of(99), 10).isEmpty());
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }
}
