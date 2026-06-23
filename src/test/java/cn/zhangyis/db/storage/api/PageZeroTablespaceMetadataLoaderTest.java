package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.fil.TablespaceType;
import cn.zhangyis.db.storage.fil.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fil.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.TablespaceAccessLease;
import cn.zhangyis.db.storage.fsp.FlstBase;
import cn.zhangyis.db.storage.fsp.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * PageZeroTablespaceMetadataLoader 测试：刷盘后 raw 读 page0 重建 metadata，未打开表空间时返回 empty。
 */
class PageZeroTablespaceMetadataLoaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(60);

    @TempDir
    Path dir;

    @Test
    void rebuildsMetadataFromDiskPageZero() {
        Path path = dir.resolve("loader.ibu");
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager mgr = new MiniTransactionManager();
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            MiniTransaction mtr = mgr.begin();
            headerRepo.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS,
                    TablespaceTypeFlags.encode(TablespaceType.UNDO), PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            headerRepo.writeLifecycle(mtr, SPACE, new TablespaceLifecycleHeader(
                    TablespaceState.ACTIVE, PageNo.of(64), 0L, PageNo.of(64), TablespaceState.ACTIVE));
            mgr.commit(mtr);
        }

        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);

            TablespaceMetadata metadata = loader.load(SPACE).orElseThrow();

            assertEquals(SPACE, metadata.spaceId());
            assertEquals(TablespaceType.UNDO, metadata.type());
            assertEquals(TablespaceState.ACTIVE, metadata.state());
            assertEquals(64L, metadata.currentSizeInPages().value());
            assertEquals(path, metadata.dataFiles().get(0).path());
        }
    }

    @Test
    void returnsEmptyForUnopenedSpace() {
        try (PageStore store = new FileChannelPageStore()) {
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertEquals(Optional.empty(), loader.load(SpaceId.of(777)));
        }
    }

    /** loader 的 raw page0 读取也必须受共享 operation lease 保护，不能跨越 truncate X。 */
    @Test
    void loaderWaitsBehindExclusiveTablespaceLease() throws Exception {
        Path path = dir.resolve("lease-loader.ibu");
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 8)) {
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager manager = new MiniTransactionManager();
            SpaceHeaderRepository repository = new SpaceHeaderRepository(pool);
            MiniTransaction mtr = manager.begin();
            repository.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS,
                    TablespaceTypeFlags.encode(TablespaceType.GENERAL), PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            manager.commit(mtr);
            pool.flushAll();

            TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS, controller);
            CompletableFuture<Optional<TablespaceMetadata>> loading;
            try (TablespaceAccessLease ignored = controller.acquireExclusive(SPACE)) {
                loading = CompletableFuture.supplyAsync(() -> loader.load(SPACE));
                TimeUnit.MILLISECONDS.sleep(100);
                assertFalse(loading.isDone());
            }
            assertEquals(TablespaceType.GENERAL, loading.get(1, TimeUnit.SECONDS).orElseThrow().type());
        }
    }
}
