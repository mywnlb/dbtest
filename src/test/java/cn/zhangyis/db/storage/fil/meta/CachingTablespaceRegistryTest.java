package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


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

    /**
     * 验证 {@code shouldLoadTablespaceFromMetadataLoaderOnFirstRequire} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void shouldLoadTablespaceFromMetadataLoaderOnFirstRequire() {
        CountingLoader loader = new CountingLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);

        TablespaceHandle handle = registry.require(SpaceId.of(10));

        assertEquals(SpaceId.of(10), handle.tablespace().spaceId());
        assertEquals(1, loader.loadCount());
    }

    /**
     * 验证 {@code shouldOpenAndFindTablespace} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void shouldOpenAndFindTablespace() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));

        TablespaceHandle opened = registry.open(SpaceId.of(10));

        assertSame(opened, registry.find(SpaceId.of(10)).orElseThrow());
        assertTrue(registry.isOpen(SpaceId.of(10)));
    }

    /**
     * 验证 {@code shouldReuseRuntimeHandleAfterFirstLoad} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void shouldReuseRuntimeHandleAfterFirstLoad() {
        CountingLoader loader = new CountingLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);

        TablespaceHandle first = registry.require(SpaceId.of(10));
        TablespaceHandle second = registry.require(SpaceId.of(10));

        assertSame(first, second);
        assertEquals(1, loader.loadCount());
    }

    /**
     * 验证 {@code shouldRejectMissingTablespace} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectMissingTablespace() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.empty());

        assertThrows(TablespaceNotFoundException.class, () -> registry.require(SpaceId.of(404)));
    }

    /**
     * 验证 {@code shouldAllowActiveTablespaceForOrdinaryRequire} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void shouldAllowActiveTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.ACTIVE)));

        TablespaceHandle handle = registry.require(SpaceId.of(10));

        assertEquals(TablespaceState.ACTIVE, handle.tablespace().state());
    }

    /**
     * 验证 {@code shouldRejectCorruptedTablespaceForOrdinaryRequire} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectCorruptedTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.CORRUPTED)));

        assertThrows(TablespaceCorruptedException.class, () -> registry.require(SpaceId.of(10)));
    }

    /**
     * 验证 {@code shouldRejectInactiveTablespaceForOrdinaryRequire} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectInactiveTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.INACTIVE)));

        assertThrows(TablespaceUnavailableException.class, () -> registry.require(SpaceId.of(10)));
    }

    /**
     * 验证 {@code shouldRejectEmptyTablespaceForOrdinaryRequire} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectEmptyTablespaceForOrdinaryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.EMPTY)));

        assertThrows(TablespaceUnavailableException.class, () -> registry.require(SpaceId.of(10)));
    }

    /**
     * 验证 {@code shouldAllowCorruptedTablespaceForRecoveryRequire} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldAllowCorruptedTablespaceForRecoveryRequire() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.CORRUPTED)));

        TablespaceHandle handle = registry.requireForRecovery(SpaceId.of(10));

        assertEquals(TablespaceState.CORRUPTED, handle.tablespace().state());
    }

    /**
     * recovery cache miss 只能发布“待普通准入复核”的句柄。第一次普通 require 必须重新调用严格 loader；严格
     * 校验失败时保留恢复句柄供诊断，修复证据后再次 require 能完成晋升，不能因 cache hit 永久绕过普通校验。
     */
    @Test
    void recoveryLoadedHandleRequiresStrictReloadBeforeOrdinaryAccess() {
        RecoveryAwareLoader loader = new RecoveryAwareLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);

        assertEquals(TablespaceState.NORMAL,
                registry.requireForRecovery(SpaceId.of(10)).tablespace().state());
        assertEquals(1, loader.recoveryLoadCount());
        assertThrows(TablespaceCorruptedException.class, () -> registry.require(SpaceId.of(10)));
        assertEquals(TablespaceState.NORMAL,
                registry.requireForRecovery(SpaceId.of(10)).tablespace().state());

        loader.allowOrdinaryLoad();
        assertEquals(TablespaceState.NORMAL, registry.require(SpaceId.of(10)).tablespace().state());
        assertEquals(2, loader.ordinaryLoadCount());
    }

    /**
     * 验证 {@code shouldRefreshRuntimeHandleFromLoader} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void shouldRefreshRuntimeHandleFromLoader() {
        MutableLoader loader = new MutableLoader(metadata(TablespaceState.NORMAL));
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(loader);
        registry.require(SpaceId.of(10));
        loader.metadata(metadata(TablespaceState.INACTIVE));

        TablespaceHandle refreshed = registry.refresh(SpaceId.of(10));

        assertEquals(TablespaceState.INACTIVE, refreshed.tablespace().state());
    }

    /**
     * 验证 {@code shouldReplaceRuntimeMetadata} 所描述的字典/DDL 协作，并断言版本、对象身份、缓存失效和物理绑定保持一致。
     */
    @Test
    void shouldReplaceRuntimeMetadata() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.empty());

        TablespaceHandle replaced = registry.replace(metadata(TablespaceState.NORMAL));

        assertSame(replaced, registry.require(SpaceId.of(10)));
    }

    /**
     * 验证 {@code shouldMarkCorruptedAndDiscarded} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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

    /**
     * 验证 {@code shouldMarkInactiveBlockingOrdinaryRequireButAllowingRecovery} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void shouldMarkInactiveBlockingOrdinaryRequireButAllowingRecovery() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));
        registry.require(SpaceId.of(10));

        TablespaceHandle inactive = registry.markInactive(SpaceId.of(10));

        assertEquals(TablespaceState.INACTIVE, inactive.tablespace().state());
        assertThrows(TablespaceUnavailableException.class, () -> registry.require(SpaceId.of(10)));
        assertEquals(TablespaceState.INACTIVE, registry.requireForRecovery(SpaceId.of(10)).tablespace().state());
        assertEquals(TablespaceState.INACTIVE, registry.find(SpaceId.of(10)).orElseThrow().tablespace().state());
    }

    /**
     * 验证 {@code shouldCloseRemoveAndListRuntimeHandles} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
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

    /** 为 recovery/ordinary 双入口提供独立计数和可恢复的严格校验失败。 */
    private static final class RecoveryAwareLoader implements TablespaceMetadataLoader {
        private final TablespaceMetadata metadata;
        private final AtomicInteger ordinaryLoads = new AtomicInteger();
        private final AtomicInteger recoveryLoads = new AtomicInteger();
        private boolean ordinaryAllowed;

        private RecoveryAwareLoader(TablespaceMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public Optional<TablespaceMetadata> load(SpaceId spaceId) {
            ordinaryLoads.incrementAndGet();
            if (!ordinaryAllowed) {
                throw new TablespaceCorruptedException("management catalog is not ready");
            }
            return Optional.of(metadata);
        }

        @Override
        public Optional<TablespaceMetadata> loadForRecovery(SpaceId spaceId) {
            recoveryLoads.incrementAndGet();
            return Optional.of(metadata);
        }

        private void allowOrdinaryLoad() {
            ordinaryAllowed = true;
        }

        private int ordinaryLoadCount() {
            return ordinaryLoads.get();
        }

        private int recoveryLoadCount() {
            return recoveryLoads.get();
        }
    }
}
