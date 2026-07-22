package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.storage.api.IndexRetirementHistoryBarrier;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import cn.zhangyis.db.storage.api.TablePurgeBarrierInterruptedException;
import cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException;

import java.time.Duration;
import java.util.List;

/**
 * 默认通用退休实现。INDEX按fence high-water等待对应history；TABLESPACE保守等待整表history；最后等待
 * exact source dictionary version pin。所有等待顺序执行且共享一次单调时钟deadline。
 */
public final class DefaultOnlineAlterRetirementBarrier
        implements OnlineAlterRetirementBarrier {

    /** transaction high-water与按index history等待入口。 */
    private final IndexRetirementHistoryBarrier history;
    /** source space retirement使用的整表persistent history屏障。 */
    private final TablePurgeBarrier tableHistory;
    /** source exact-version metadata pin owner。 */
    private final DictionaryObjectCache cache;

    /**
     * @param history 与TransactionSystem/purge共享owner的索引history屏障
     * @param tableHistory 与persistent history list共享owner的整表屏障
     * @param cache live/recovery共享的版本化dictionary cache
     */
    public DefaultOnlineAlterRetirementBarrier(
            IndexRetirementHistoryBarrier history,
            TablePurgeBarrier tableHistory,
            DictionaryObjectCache cache) {
        if (history == null || tableHistory == null || cache == null) {
            throw new DatabaseValidationException(
                    "online ALTER retirement barrier requires history/table/cache");
        }
        this.history = history;
        this.tableHistory = tableHistory;
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override
    public DdlRetirementFence captureFence(
            long tableId, long sourceDictionaryVersion,
            long descriptorGeneration, long ownerDdlId,
            List<DdlRetiredResource> resources) {
        if (tableId <= 0 || sourceDictionaryVersion <= 0
                || descriptorGeneration <= 0 || ownerDdlId <= 0
                || resources == null || resources.isEmpty()) {
            throw new DatabaseValidationException(
                    "online ALTER retirement fence identities/resources are invalid");
        }
        // DdlRetirementFence构造器负责canonical排序校验；此处只在全部逻辑identity合法后读取counter。
        long highWater = history.captureTransactionHighWater();
        return new DdlRetirementFence(tableId, sourceDictionaryVersion, highWater,
                sourceDictionaryVersion, descriptorGeneration, ownerDdlId, resources);
    }

    /**
     * {@inheritDoc}
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验fence/timeout并冻结单一绝对deadline，拒绝未知资源类别。</li>
     *     <li>按canonical资源顺序等待INDEX high-water或TABLESPACE整表history；每阶段只消费剩余预算。</li>
     *     <li>全部history锁已释放后等待exact source-version pin，成功才允许物理回收。</li>
     * </ol>
     */
    @Override
    public void awaitSafe(DdlRetirementFence fence, Duration timeout) {
        // 1、所有结构校验早于阻塞，deadline只计算一次。
        if (fence == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "online ALTER retirement requires fence and positive timeout");
        }
        long started = System.nanoTime();
        long budget = boundedTimeoutNanos(timeout);

        // 2、history实现各自使用显式Condition；调用间不嵌套任何history/cache锁。
        for (DdlRetiredResource resource : fence.resources()) {
            Duration remaining = remainingDuration(started, budget, fence);
            try {
                if (resource.kind() == DdlRetiredResourceKind.INDEX) {
                    history.awaitIndexHistorySafe(fence.tableId(), resource.resourceId(),
                            fence.retireThroughTransactionNo(), remaining);
                } else if (resource.kind() == DdlRetiredResourceKind.TABLESPACE) {
                    tableHistory.awaitUnreferenced(fence.tableId(), remaining);
                } else {
                    throw new DatabaseValidationException(
                            "unknown online ALTER retired resource kind: " + resource.kind());
                }
            } catch (TablePurgeBarrierTimeoutException
                     | TablePurgeBarrierInterruptedException failure) {
                throw new OnlineDdlRetirementTimeoutException(
                        "online ALTER retired resource is not history-safe: table="
                                + fence.tableId() + " resource=" + resource, failure);
            }
        }

        // 3、target新pin不影响source exact version；失败只保留fence/资源供recovery前滚。
        Duration remaining = remainingDuration(started, budget, fence);
        if (!cache.awaitVersionUnpinned(TableId.of(fence.tableId()),
                DictionaryVersion.of(fence.sourceMetadataPinVersion()), remaining)) {
            throw new OnlineDdlRetirementTimeoutException(
                    "online ALTER source metadata pin is not released: table="
                            + fence.tableId() + " version="
                            + fence.sourceMetadataPinVersion());
        }
    }

    /** 把超大Duration饱和到Condition可表达的正纳秒预算。 */
    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** 从同一deadline计算下一阶段剩余预算；耗尽时立即进入只能前滚的退休超时。 */
    private static Duration remainingDuration(
            long started, long budget, DdlRetirementFence fence) {
        long elapsed = System.nanoTime() - started;
        if (elapsed < 0 || elapsed >= budget) {
            throw new OnlineDdlRetirementTimeoutException(
                    "online ALTER retirement deadline expired: table=" + fence.tableId());
        }
        return Duration.ofNanos(budget - elapsed);
    }
}
