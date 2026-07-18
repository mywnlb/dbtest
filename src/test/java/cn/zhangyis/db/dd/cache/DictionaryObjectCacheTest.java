package cn.zhangyis.db.dd.cache;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Dictionary object cache 的 single-flight、版本 publish 和 pin 生命周期 TDD。 */
class DictionaryObjectCacheTest {

    /** 同一 table miss 只有 leader 执行 repository loader，等待线程只挂 CompletableFuture 且有 timeout。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void coalescesConcurrentLoadsWithoutHoldingCacheLockDuringIo() throws Exception {
        DictionaryObjectCache cache = new DictionaryObjectCache(16);
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<DictionaryPin<TableDefinition>> first = executor.submit(() -> cache.pinTable(TableId.of(2),
                    Duration.ofSeconds(2), () -> {
                        loads.incrementAndGet();
                        loaderEntered.countDown();
                        releaseLoader.await();
                        return Optional.of(table(2));
                    }));
            loaderEntered.await();
            Future<DictionaryPin<TableDefinition>> second = executor.submit(() -> cache.pinTable(TableId.of(2),
                    Duration.ofSeconds(2), () -> {
                        loads.incrementAndGet();
                        return Optional.of(table(2));
                    }));
            releaseLoader.countDown();

            try (DictionaryPin<TableDefinition> pin1 = first.get();
                 DictionaryPin<TableDefinition> pin2 = second.get()) {
                assertEquals(1, loads.get());
                assertEquals(DictionaryVersion.of(2), pin1.version());
                assertEquals(pin1.value(), pin2.value());
            }
        }
    }

    /** publish 不原地修改已 pin 对象：旧 pin 标 stale，新 pin 看新版本，旧版本最后一个 pin 释放后才回收。 */
    @Test
    void keepsPinnedOldVersionStaleUntilRelease() {
        DictionaryObjectCache cache = new DictionaryObjectCache(16);
        cache.publishTable(table(2));
        DictionaryPin<TableDefinition> oldPin = cache.pinTable(TableId.of(2), Duration.ofSeconds(1), Optional::empty);

        cache.publishTable(table(3));

        assertTrue(oldPin.stale());
        try (DictionaryPin<TableDefinition> newPin = cache.pinTable(TableId.of(2),
                Duration.ofSeconds(1), Optional::empty)) {
            assertEquals(DictionaryVersion.of(3), newPin.version());
            assertFalse(newPin.stale());
            assertEquals(2, cache.snapshot().versionCount());
        }
        oldPin.close();
        assertEquals(1, cache.snapshot().versionCount());
        assertTrue(cache.awaitUnpinned(TableId.of(2), Duration.ofSeconds(1)));
    }

    /** leader 发生 JVM Error 时也必须清掉 LOADING 并原样传播，不得包装成可重试的字典运行异常。 */
    @Test
    void propagatesLoaderErrorAndClearsSingleFlightState() {
        DictionaryObjectCache cache = new DictionaryObjectCache(16);

        assertThrows(AssertionError.class, () -> cache.pinTable(TableId.of(2), Duration.ofSeconds(1),
                () -> { throw new AssertionError("injected fatal loader error"); }));
        assertEquals(0, cache.snapshot().loadingCount());
    }

    /** DROP admission barrier 建立后，即使 repository 仍返回旧 ACTIVE 版本，也不得被 cache miss 重新发布。 */
    @Test
    void invalidationBlocksReloadOfOlderActiveVersion() {
        DictionaryObjectCache cache = new DictionaryObjectCache(16);
        cache.publishTable(table(2));
        cache.invalidateTable(TableId.of(2), DictionaryVersion.of(3));

        assertThrows(DictionaryObjectNotFoundException.class, () -> cache.pinTable(TableId.of(2),
                Duration.ofSeconds(1), () -> Optional.of(table(2))));
        assertEquals(0, cache.snapshot().loadingCount());
    }

    private static TableDefinition table(long version) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(version), TableState.ACTIVE, List.of(id), List.of(primary));
    }
}
