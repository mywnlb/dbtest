package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CachingTablespaceRegistry 测试固定“磁盘/DD/配置是权威来源，内存 registry 只缓存运行时句柄”的边界，
 * 并固定普通 IO 路径的状态白名单（仅 NORMAL/ACTIVE 可访问）。
 */
class CachingTablespaceRegistryTest {

    @Test
    void shouldLoadTablespaceFromMetadataLoaderOnFirstRequire() {
        CountingLoader loader = new CountingLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);

        TablespaceHandle handle = registry.require(SpaceId.of(10));

        assertEquals(SpaceId.of(10), handle.tablespace().spaceId());
        assertEquals(1, loader.loadCount());
    }

    @Test
    void shouldOpenAndFindTablespace() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));

        TablespaceHandle opened = registry.open(SpaceId.of(10));

        assertSame(opened, registry.find(SpaceId.of(10)).orElseThrow());
        assertTrue(registry.isOpen(SpaceId.of(10)));
    }

    @Test
    void shouldReuseRuntimeHandleAfterFirstLoad() {
        CountingLoader loader = new CountingLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);

        TablespaceHandle first = registry.require(SpaceId.of(10));
        TablespaceHandle second = registry.require(SpaceId.of(10));

        assertSame(first, second);
        assertEquals(1, loader.loadCount());
    }

    @Test
    void shouldRejectMissingTablespace() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.empty());

        assertThrows(TablespaceNotFoundException.class, () -> registry.require(SpaceId.of(404)));
    }

    @Test
    void shouldAllowActiveTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.ACTIVE)));

        TablespaceHandle handle = registry.require(SpaceId.of(10));

        assertEquals(TablespaceState.ACTIVE, handle.tablespace().state());
    }

    @Test
    void shouldRejectCorruptedTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.CORRUPTED)));

        assertThrows(TablespaceCorruptedException.class, () -> registry.require(SpaceId.of(10)));
    }

    @Test
    void shouldRejectInactiveTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.INACTIVE)));

        assertThrows(TablespaceUnavailableException.class, () -> registry.require(SpaceId.of(10)));
    }

    @Test
    void shouldRejectEmptyTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.EMPTY)));

        assertThrows(TablespaceUnavailableException.class, () -> registry.require(SpaceId.of(10)));
    }

    @Test
    void shouldAllowCorruptedTablespaceForRecoveryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.CORRUPTED)));

        TablespaceHandle handle = registry.requireForRecovery(SpaceId.of(10));

        assertEquals(TablespaceState.CORRUPTED, handle.tablespace().state());
    }

    @Test
    void shouldRefreshRuntimeHandleFromLoader() {
        MutableLoader loader = new MutableLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);
        registry.require(SpaceId.of(10));
        loader.metadata(metadata(TablespaceState.INACTIVE));

        TablespaceHandle refreshed = registry.refresh(SpaceId.of(10));

        assertEquals(TablespaceState.INACTIVE, refreshed.tablespace().state());
    }

    @Test
    void shouldReplaceRuntimeMetadata() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.empty());

        TablespaceHandle replaced = registry.replace(metadata(TablespaceState.NORMAL));

        assertSame(replaced, registry.require(SpaceId.of(10)));
    }

    @Test
    void shouldMarkCorruptedAndDiscarded() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));

        TablespaceHandle corrupted = registry.markCorrupted(SpaceId.of(10), "checksum mismatch");

        assertEquals(TablespaceState.CORRUPTED, corrupted.tablespace().state());
        assertThrows(TablespaceCorruptedException.class, () -> registry.require(SpaceId.of(10)));

        CachingTablespaceRegistry another = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));
        TablespaceHandle discarded = another.markDiscarded(SpaceId.of(10));

        assertEquals(TablespaceState.DISCARDED, discarded.tablespace().state());
        assertThrows(TablespaceNotFoundException.class, () -> another.require(SpaceId.of(10)));
    }

    @Test
    void shouldCloseRemoveAndListRuntimeHandles() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));
        registry.open(SpaceId.of(10));

        assertEquals(1, registry.listOpenTablespaces().size());

        registry.close(SpaceId.of(10));

        assertFalse(registry.isOpen(SpaceId.of(10)));

        registry.open(SpaceId.of(10));
        registry.remove(SpaceId.of(10));

        assertFalse(registry.find(SpaceId.of(10)).isPresent());
    }

    private TablespaceMetadata metadata(TablespaceState state) {
        DataFileDescriptor dataFile = DataFileDescriptor.single(Path.of("t1.ibd"), PageNo.of(0), PageNo.of(128));
        return new TablespaceMetadata(SpaceId.of(10), "user/t1", TablespaceType.FILE_PER_TABLE,
                PageSize.ofBytes(16 * 1024), state, List.of(dataFile), SpaceFlags.empty(), PageNo.of(128), PageNo.of(128), 1);
    }

    private static final class CountingLoader implements TablespaceMetadataLoader {
        private final TablespaceMetadata metadata;
        private final AtomicInteger loadCount = new AtomicInteger();

        private CountingLoader(TablespaceMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public Optional<TablespaceMetadata> load(SpaceId spaceId) {
            loadCount.incrementAndGet();
            return Optional.of(metadata);
        }

        private int loadCount() {
            return loadCount.get();
        }
    }

    private static final class MutableLoader implements TablespaceMetadataLoader {
        private TablespaceMetadata metadata;

        private MutableLoader(TablespaceMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public Optional<TablespaceMetadata> load(SpaceId spaceId) {
            return Optional.of(metadata);
        }

        private void metadata(TablespaceMetadata metadata) {
            this.metadata = metadata;
        }
    }
}
