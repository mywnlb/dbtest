package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flst 集成测试（scratch 页驱动）：空表头/尾插、顺序遍历、头/尾/中删、length 维护、跨页 add/remove（锁序 page 小→大）、
 * 非法地址拒绝、空表 remove 拒绝。节点放在数据页固定槽位（每槽 24B，互不重叠、≥FIL_PAGE_DATA、非 (0,0)）。
 */
class FlstTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    // 单页场景：base 与 node 均在 page 3
    private static final FileAddress BASE = FileAddress.of(PageNo.of(3), 38);
    private static final FileAddress A = FileAddress.of(PageNo.of(3), 100);
    private static final FileAddress B = FileAddress.of(PageNo.of(3), 200);
    private static final FileAddress C = FileAddress.of(PageNo.of(3), 300);

    private interface Body {
        void run(Flst flst, MiniTransaction mtr);
    }

    private void withFlst(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(5));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            Flst flst = new Flst(pool);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(flst, mtr);
            mgr.commit(mtr);
        }
    }

    /**
     * 验证 {@code addLastBuildsInsertionOrder} 所描述的字典/DDL 协作，并断言版本、对象身份、缓存失效和物理绑定保持一致。
     */
    @Test
    void addLastBuildsInsertionOrder() {
        withFlst((flst, mtr) -> {
            flst.addLast(mtr, SPACE, BASE, A);
            flst.addLast(mtr, SPACE, BASE, B);
            flst.addLast(mtr, SPACE, BASE, C);
            assertEquals(3L, flst.length(mtr, SPACE, BASE));
            assertEquals(A, flst.getFirst(mtr, SPACE, BASE));
            assertEquals(C, flst.getLast(mtr, SPACE, BASE));
            assertEquals(B, flst.getNext(mtr, SPACE, A));
            assertEquals(C, flst.getNext(mtr, SPACE, B));
            assertTrue(flst.getNext(mtr, SPACE, C).isNull());
            assertEquals(B, flst.getPrev(mtr, SPACE, C));
            assertTrue(flst.getPrev(mtr, SPACE, A).isNull());
        });
    }

    /**
     * 验证 {@code addFirstBuildsReverseOrder} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void addFirstBuildsReverseOrder() {
        withFlst((flst, mtr) -> {
            flst.addFirst(mtr, SPACE, BASE, A);
            flst.addFirst(mtr, SPACE, BASE, B);
            assertEquals(2L, flst.length(mtr, SPACE, BASE));
            assertEquals(B, flst.getFirst(mtr, SPACE, BASE));
            assertEquals(A, flst.getLast(mtr, SPACE, BASE));
            assertEquals(A, flst.getNext(mtr, SPACE, B));
        });
    }

    /**
     * 验证 {@code removeHeadMiddleTailMaintainsLinks} 所描述的字典/DDL 协作，并断言版本、对象身份、缓存失效和物理绑定保持一致。
     */
    @Test
    void removeHeadMiddleTailMaintainsLinks() {
        withFlst((flst, mtr) -> {
            flst.addLast(mtr, SPACE, BASE, A);
            flst.addLast(mtr, SPACE, BASE, B);
            flst.addLast(mtr, SPACE, BASE, C);
            // 中删 B
            flst.remove(mtr, SPACE, BASE, B);
            assertEquals(2L, flst.length(mtr, SPACE, BASE));
            assertEquals(C, flst.getNext(mtr, SPACE, A));
            assertEquals(A, flst.getPrev(mtr, SPACE, C));
            assertTrue(flst.getNext(mtr, SPACE, B).isNull());
            assertTrue(flst.getPrev(mtr, SPACE, B).isNull());
            // 头删 A
            flst.remove(mtr, SPACE, BASE, A);
            assertEquals(C, flst.getFirst(mtr, SPACE, BASE));
            assertTrue(flst.getPrev(mtr, SPACE, C).isNull());
            // 尾删 C → 空链
            flst.remove(mtr, SPACE, BASE, C);
            assertEquals(0L, flst.length(mtr, SPACE, BASE));
            assertTrue(flst.getFirst(mtr, SPACE, BASE).isNull());
            assertTrue(flst.getLast(mtr, SPACE, BASE).isNull());
        });
    }

    /**
     * 验证 {@code crossPageAddRemoveRespectsLockOrder} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void crossPageAddRemoveRespectsLockOrder() {
        // base 在 page4、node 在 page3：np(3) < bp(4)，Flst 应先取 page3 再取 page4（升序），不死锁、不残留阻塞线程。
        FileAddress base = FileAddress.of(PageNo.of(4), 38);
        FileAddress n1 = FileAddress.of(PageNo.of(3), 100);
        FileAddress n2 = FileAddress.of(PageNo.of(3), 200);
        withFlst((flst, mtr) -> {
            flst.addLast(mtr, SPACE, base, n1);
            flst.addLast(mtr, SPACE, base, n2);
            assertEquals(2L, flst.length(mtr, SPACE, base));
            assertEquals(n1, flst.getFirst(mtr, SPACE, base));
            assertEquals(n2, flst.getLast(mtr, SPACE, base));
            flst.remove(mtr, SPACE, base, n1);
            assertEquals(1L, flst.length(mtr, SPACE, base));
            assertEquals(n2, flst.getFirst(mtr, SPACE, base));
        });
    }

    /**
     * 验证 {@code rejectsNullAndEmptyRemove} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNullAndEmptyRemove() {
        withFlst((flst, mtr) -> {
            assertThrows(DatabaseValidationException.class, () -> flst.addLast(mtr, SPACE, FileAddress.NULL, A));
            assertThrows(DatabaseValidationException.class, () -> flst.addLast(mtr, SPACE, BASE, FileAddress.NULL));
            assertThrows(FspMetadataException.class, () -> flst.remove(mtr, SPACE, BASE, A));
        });
    }
}
