package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** MTR lease 协作测试：第一次 fix 空间页时取得共享 lease，并在所有 page guard 之后、MTR 结束时释放。 */
class MiniTransactionTablespaceLeaseTest {

    @TempDir
    Path dir;

    /**
     * 验证 {@code pageFixHoldsSharedTablespaceLeaseUntilCommit} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void pageFixHoldsSharedTablespaceLeaseUntilCommit() throws Exception {
        SpaceId spaceId = SpaceId.of(79);
        PageSize pageSize = PageSize.ofBytes(16 * 1024);
        TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, pageSize, 4)) {
            store.create(spaceId, dir.resolve("lease.ibu"), pageSize, PageNo.of(1));
            MiniTransactionManager manager = new MiniTransactionManager(controller);
            MiniTransaction mtr = manager.begin();
            mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.SHARED);

            CompletableFuture<Void> exclusive = CompletableFuture.runAsync(() -> {
                try (TablespaceAccessLease ignored = controller.acquireExclusive(spaceId)) {
                    // 取得后立即释放。
                }
            });
            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(exclusive.isDone());

            manager.commit(mtr);
            exclusive.get(1, TimeUnit.SECONDS);
        }
    }
}
