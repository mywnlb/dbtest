package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.storage.api.IndexRetirementHistoryBarrier;
import cn.zhangyis.db.storage.api.TablePurgeBarrierInterruptedException;
import cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException;

import java.time.Duration;
import java.util.List;

/**
 * 默认Online DROP退休屏障：先等待fence高水位内persistent history，再等待exact source dictionary version pin。
 * 两个协作者顺序调用且从不嵌套持锁。
 */
public final class DefaultIndexRetirementBarrier implements IndexRetirementBarrier {

    /** 事务提交号与persistent history的稳定storage API。 */
    private final IndexRetirementHistoryBarrier history;
    /** 版本化DD cache的exact pin owner。 */
    private final DictionaryObjectCache cache;

    /**
     * 绑定同一DatabaseEngine实例的history与cache；对象只保存引用，不复制权威计数。
     *
     * @param history 已由事务恢复重建且与purge worker共享owner的退休API
     * @param cache live/recovery共同发布metadata version的cache
     * @throws DatabaseValidationException 任一依赖为空时抛出
     */
    public DefaultIndexRetirementBarrier(IndexRetirementHistoryBarrier history,
                                         DictionaryObjectCache cache) {
        if (history == null || cache == null) {
            throw new DatabaseValidationException(
                    "default index retirement barrier requires history/cache");
        }
        this.history = history;
        this.cache = cache;
    }

    /**
     * 捕获事务高水位并冻结单INDEX资源fence；方法调用时上层必须已持final X且gate已排空source owner。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验table/version/index/descriptor/owner identity，避免构造无法与marker交叉验证的fence。</li>
     *     <li>短读TransactionSystem的next counter并转换为最后已分配提交号，不消费新号码。</li>
     *     <li>把source version同时作为metadata pin version，冻结唯一有序INDEX资源后返回。</li>
     * </ol>
     *
     * @param tableId final X下的稳定正表identity
     * @param sourceDictionaryVersion source aggregate正版本
     * @param indexId 待删除普通二级索引正identity
     * @param descriptorGeneration 当前page3 descriptor正代际
     * @param ownerDdlId 当前marker正identity
     * @return 字段完整、资源有序且可持久化的retirement fence
     * @throws DatabaseValidationException 任一identity无效时抛出且不读取事务counter
     */
    @Override
    public DdlRetirementFence captureIndexFence(long tableId, long sourceDictionaryVersion,
                                                long indexId, long descriptorGeneration,
                                                long ownerDdlId) {
        // 1. 先校验全部DD/descriptor identity，避免无效调用也读取并传播诊断高水位。
        if (tableId <= 0L || sourceDictionaryVersion <= 0L || indexId <= 0L
                || descriptorGeneration <= 0L || ownerDdlId <= 0L) {
            throw new DatabaseValidationException(
                    "index retirement fence identities must be positive");
        }

        // 2. storage短锁只读取counter；final X/gate quiescence保证source writer不会在捕获点后才取得旧提交号。
        long highWater = history.captureTransactionHighWater();

        // 3. 单Online DROP只有一个INDEX资源；List天然满足fence的stable kind/id排序与去重约束。
        return new DdlRetirementFence(tableId, sourceDictionaryVersion, highWater,
                sourceDictionaryVersion, descriptorGeneration, ownerDdlId,
                List.of(new DdlRetiredResource(DdlRetiredResourceKind.INDEX, indexId)));
    }

    /**
     * 顺序等待persistent history与exact source pin；target version新事务不会扩大任一谓词。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验fence只含一个INDEX资源及正timeout，并把超大Duration饱和为单一绝对纳秒预算。</li>
     *     <li>等待高水位内history退出；当前v1 storage投影按table保守等待，覆盖目标index但可能等待更多记录。</li>
     *     <li>前一步释放history lock后只用同一预算的剩余部分等待exact source-version pin；超时统一转换为前滚恢复异常。</li>
     * </ol>
     *
     * @param fence 已持久安装在DROP_INDEX marker的单index边界
     * @param timeout history与source pin等待共同使用的正总预算；不是每个阶段各自重新计时
     * @throws DatabaseValidationException fence或timeout无效时抛出
     * @throws OnlineDdlRetirementTimeoutException history/pin未及时安全或等待中断时抛出，segment必须保留
     */
    @Override
    public void awaitIndexSafe(DdlRetirementFence fence, Duration timeout) {
        // 1. operation-specific验证早于任何阻塞；随后冻结一次总预算，两个下游等待不得各自重置timeout。
        if (fence == null || timeout == null || timeout.isZero() || timeout.isNegative()
                || fence.resources().size() != 1
                || fence.resources().getFirst().kind() != DdlRetiredResourceKind.INDEX) {
            throw new DatabaseValidationException(
                    "single-index retirement requires INDEX fence/positive timeout");
        }
        long indexId = fence.resources().getFirst().resourceId();
        long startedNanos = System.nanoTime();
        long budgetNanos = boundedTimeoutNanos(timeout);

        // 2. history wait不持cache lock；timeout/interrupt保留cause并明确进入只能前滚的retirement失败。
        try {
            history.awaitIndexHistorySafe(fence.tableId(), indexId,
                    fence.retireThroughTransactionNo(), Duration.ofNanos(budgetNanos));
        } catch (TablePurgeBarrierTimeoutException | TablePurgeBarrierInterruptedException failure) {
            throw new OnlineDdlRetirementTimeoutException(
                    "online DDL retirement history is not safe: table=" + fence.tableId()
                            + " index=" + indexId, failure);
        }

        // 3. target metadata pin与新history允许继续增长；只观察source version且复用history消耗后的剩余预算。
        long remainingNanos = remainingNanos(startedNanos, budgetNanos);
        if (remainingNanos <= 0L) {
            throw new OnlineDdlRetirementTimeoutException(
                    "online DDL retirement deadline expired after history wait: table="
                            + fence.tableId() + " index=" + indexId);
        }
        if (!cache.awaitVersionUnpinned(TableId.of(fence.tableId()),
                DictionaryVersion.of(fence.sourceMetadataPinVersion()),
                Duration.ofNanos(remainingNanos))) {
            throw new OnlineDdlRetirementTimeoutException(
                    "online DDL source metadata pin is not released: table=" + fence.tableId()
                            + " version=" + fence.sourceMetadataPinVersion());
        }
    }

    /** Duration超出Condition纳秒表达范围时饱和；正时限不能因表示层溢出变成校验失败。 */
    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** 根据单调时钟计算总预算剩余量；只服务本次顺序等待，不把墙上时钟调整带入并发语义。 */
    private static long remainingNanos(long startedNanos, long budgetNanos) {
        long elapsedNanos = System.nanoTime() - startedNanos;
        if (elapsedNanos < 0L || elapsedNanos >= budgetNanos) {
            return 0L;
        }
        return budgetNanos - elapsedNanos;
    }
}
