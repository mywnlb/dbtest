package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.fil.TablespaceType;
import cn.zhangyis.db.storage.fil.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DiskSpaceManager 表空间准入测试：建表空间注册 NORMAL、typed create 写 flags、recovery 可从 page0 重开 metadata。
 */
class DiskSpaceManagerTablespaceAdmissionTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    @Test
    void createUndoTablespaceRegistersActiveAndAllowsSpaceManagement() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(50);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("a.ibu"), PageNo.of(64), TablespaceType.UNDO);
            mgr.commit(boot);

            assertEquals(TablespaceState.ACTIVE, disk.tablespaceState(space));

            MiniTransaction mtr = mgr.begin();
            SegmentRef segment = disk.createSegment(mtr, space, SegmentPurpose.UNDO);
            disk.allocatePage(mtr, segment);
            mgr.commit(mtr);
        });
    }

    @Test
    void defaultCreateTablespaceUsesGeneralType() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(51);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("b.ibu"), PageNo.of(64));
            mgr.commit(boot);

            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(space));
        });
    }

    @Test
    void openTablespaceForRecoveryReopensNormalFromDisk() {
        Path path = dir.resolve("rec.ibu");
        SpaceId space = SpaceId.of(52);
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, path, PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
        }
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespaceForRecovery(space, path);

            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(space));
        }
    }

    @Test
    void markCorruptedBlocksCreateSegment() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(60);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("c.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);

            disk.markTablespaceCorrupted(space, "checksum mismatch");

            assertEquals(TablespaceState.CORRUPTED, disk.tablespaceState(space));
            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceCorruptedException.class,
                    () -> disk.createSegment(mtr, space, SegmentPurpose.INDEX_LEAF));
            mgr.rollbackUncommitted(mtr);
        });
    }

    @Test
    void markInactiveBlocksAllocatePage() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(61);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("i.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            SegmentRef segment = disk.createSegment(boot, space, SegmentPurpose.INDEX_LEAF);
            mgr.commit(boot);

            disk.markTablespaceInactive(space);

            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceUnavailableException.class, () -> disk.allocatePage(mtr, segment));
            mgr.rollbackUncommitted(mtr);
        });
    }

    @Test
    void discardBlocksUsage() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(62);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("d.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);

            disk.discardTablespace(space);

            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceNotFoundException.class, () -> disk.usage(mtr, space));
            mgr.rollbackUncommitted(mtr);
        });
    }

    @Test
    void reopenViaDirectStoreOpenLazyLoadsViaLoader() {
        Path path = dir.resolve("re.ibu");
        SpaceId space = SpaceId.of(63);
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, path, PageNo.of(64), TablespaceType.UNDO);
            mgr.commit(boot);
        }
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(space, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction mtr = mgr.begin();
            SegmentRef segment = disk.createSegment(mtr, space, SegmentPurpose.UNDO);
            disk.allocatePage(mtr, segment);
            mgr.commit(mtr);

            assertEquals(TablespaceState.ACTIVE, disk.tablespaceState(space));
        }
    }

    private interface Body {
        void run(MiniTransactionManager mgr, DiskSpaceManager disk, PageStore store);
    }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            body.run(mgr, disk, store);
        }
    }
}
