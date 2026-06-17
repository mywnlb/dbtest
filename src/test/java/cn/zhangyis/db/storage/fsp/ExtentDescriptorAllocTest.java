package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExtentDescriptorRepository 2b 扩展测试：节点地址互转、bitmap 首空/计数/满空判定。
 */
class ExtentDescriptorAllocTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(7);

    @TempDir
    Path dir;

    private interface Body {
        void run(ExtentDescriptorRepository repo, MiniTransaction mtr);
    }

    private void withRepo(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(128));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            ExtentDescriptorRepository repo = new ExtentDescriptorRepository(pool, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            body.run(repo, mtr);
            mgr.commit(mtr);
        }
    }

    @Test
    void nodeAddrRoundTrip() {
        withRepo((repo, mtr) -> {
            ExtentId e = ExtentId.of(SPACE, 1);
            FileAddress node = repo.listNodeAddr(e);
            assertEquals(e, repo.extentIdOfNode(SPACE, node));
            ExtentId e2 = ExtentId.of(SPACE, 0);
            assertEquals(e2, repo.extentIdOfNode(SPACE, repo.listNodeAddr(e2)));
        });
    }

    @Test
    void extentIdOfNodeRejectsBadOffsetOrPage() {
        withRepo((repo, mtr) -> {
            assertThrows(FspMetadataException.class,
                    () -> repo.extentIdOfNode(SPACE, FileAddress.of(PageNo.of(0), 999999)));
            assertThrows(FspMetadataException.class,
                    () -> repo.extentIdOfNode(SPACE, FileAddress.of(PageNo.of(1), 300)));
        });
    }

    @Test
    void bitmapQueriesReflectAllocation() {
        withRepo((repo, mtr) -> {
            ExtentId e = ExtentId.of(SPACE, 1);
            repo.initFree(mtr, e);
            assertTrue(repo.isEmpty(mtr, e));
            assertFalse(repo.isFull(mtr, e));
            assertEquals(0, repo.allocatedPageCount(mtr, e));
            assertEquals(OptionalInt.of(0), repo.firstFreePageIndex(mtr, e));

            repo.setPageAllocated(mtr, e, 0, true);
            repo.setPageAllocated(mtr, e, 1, true);
            assertEquals(2, repo.allocatedPageCount(mtr, e));
            assertEquals(OptionalInt.of(2), repo.firstFreePageIndex(mtr, e));

            int pe = PS.pagesPerExtent();
            for (int i = 0; i < pe; i++) {
                repo.setPageAllocated(mtr, e, i, true);
            }
            assertTrue(repo.isFull(mtr, e));
            assertEquals(OptionalInt.empty(), repo.firstFreePageIndex(mtr, e));
            assertEquals(pe, repo.allocatedPageCount(mtr, e));
        });
    }
}
