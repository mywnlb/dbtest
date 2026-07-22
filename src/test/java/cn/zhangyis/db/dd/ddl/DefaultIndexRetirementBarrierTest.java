package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.cache.DictionaryPin;
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
import cn.zhangyis.db.storage.api.IndexRetirementHistoryBarrier;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证Online DROP退休屏障在history与metadata pin之间共享一个绝对等待预算。 */
class DefaultIndexRetirementBarrierTest {

    /** history已消耗的时间必须从pin等待中扣除，不能让一次退休调用等待接近两个timeout。 */
    @Test
    void sharesOneTimeoutBudgetAcrossHistoryAndMetadataPins() {
        DictionaryObjectCache cache = new DictionaryObjectCache(8);
        cache.publishTable(table(2));
        IndexRetirementHistoryBarrier delayedHistory = new IndexRetirementHistoryBarrier() {
            @Override
            public long captureTransactionHighWater() {
                return 9L;
            }

            @Override
            public void awaitIndexHistorySafe(long tableId, long indexId,
                                              long retireThroughTransactionNo, Duration timeout) {
                try {
                    Thread.sleep(180L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("test history delay was interrupted", interrupted);
                }
            }
        };
        DefaultIndexRetirementBarrier barrier =
                new DefaultIndexRetirementBarrier(delayedHistory, cache);
        DdlRetirementFence fence = barrier.captureIndexFence(2L, 2L, 4L, 3L, 7L);

        long started = System.nanoTime();
        try (DictionaryPin<TableDefinition> ignored = cache.pinTable(
                TableId.of(2), Duration.ofSeconds(1), Optional::empty)) {
            assertThrows(OnlineDdlRetirementTimeoutException.class,
                    () -> barrier.awaitIndexSafe(fence, Duration.ofMillis(250)));
        }
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();

        assertTrue(elapsedMillis < 360L,
                "history and pin waits exceeded the shared deadline: " + elapsedMillis + "ms");
    }

    private static TableDefinition table(long version) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"),
                true, true, List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(version), TableState.ACTIVE, List.of(id), List.of(primary));
    }
}
