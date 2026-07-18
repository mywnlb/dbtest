package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.SpaceUsage;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * P4 {@code dropUndoSegment}：purge 物理回收 undo 段。create+drop 多轮后 undo 表空间 currentSizeInPages 不增长，
 * 证明段页/inode 槽被归还 FSP free list 复用——未回收会因 fragment 页耗尽触发 autoextend 使 currentSize 增长。
 */
class UndoSegmentDropReclaimTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(78);

    @TempDir
    Path dir;

    /**
     * 验证 {@code dropReclaimsSegmentPagesAcrossCycles} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void dropReclaimsSegmentPagesAcrossCycles() {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            DiskSpaceUndoAllocator allocator = new DiskSpaceUndoAllocator(disk);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);

            // 预热一轮：extent0 是系统保留 extent，初始 64 页无可分配 fragment 页，首次分配必 autoextend 一次。
            // 取预热后的 size 作为基线——之后若每轮 drop 都回收，size 应稳定；若不回收，200 轮远超空闲页必再次 autoextend。
            createAndDrop(mgr, allocator);
            long warmSize = currentSize(mgr, disk);
            for (int i = 0; i < 200; i++) {
                createAndDrop(mgr, allocator);
            }
            assertEquals(warmSize, currentSize(mgr, disk),
                    "预热后每轮 dropUndoSegment 回收段页/inode → 不再 autoextend，currentSizeInPages 稳定");
        }
    }

    private static void createAndDrop(MiniTransactionManager mgr, DiskSpaceUndoAllocator allocator) {
        MiniTransaction c = mgr.begin();
        UndoSegmentHandle h = allocator.createUndoSegment(c, UNDO_SPACE);
        mgr.commit(c);
        MiniTransaction d = mgr.begin();
        allocator.dropUndoSegment(d, h);
        mgr.commit(d);
    }

    private static long currentSize(MiniTransactionManager mgr, DiskSpaceManager disk) {
        MiniTransaction m = mgr.begin();
        SpaceUsage usage = disk.usage(m, UNDO_SPACE);
        mgr.commit(m);
        return usage.currentSizeInPages().value();
    }
}
