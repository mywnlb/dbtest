package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.List;

/**
 * Redo 恢复读取器：启动恢复阶段顺序扫描 redo 文件，输出完整批次，并记录扫描到的 recoveredToLsn。
 */
public final class RedoRecoveryReader {

    /** redo 文件仓储。 */
    private final RedoLogFileRepository repository;
    /** 已持久化 checkpoint LSN；被覆盖的完整旧批次不再交给 replay。 */
    private final Lsn checkpointLsn;
    /** 最近一次扫描得到的恢复终点。 */
    private Lsn recoveredToLsn = Lsn.of(0);

    /**
     * 创建 {@code RedoRecoveryReader}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    public RedoRecoveryReader(RedoLogFileRepository repository) {
        this(repository, Lsn.of(0));
    }

    /**
     * 创建从指定 checkpoint 后恢复的 reader。reader 从 repository retained 边界顺序扫描完整 block chain，但只返回
     * {@code batch.endLsn > checkpointLsn} 的批次；若 checkpoint 落在某个批次内部，则保留该批次并交给 pageLSN 幂等判断。
     *
     * @param repository redo 文件仓储。
     * @param checkpointLsn 持久化 checkpoint label 中的恢复起点。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoRecoveryReader(RedoLogFileRepository repository, Lsn checkpointLsn) {
        if (repository == null) {
            throw new DatabaseValidationException("redo log repository must not be null");
        }
        if (checkpointLsn == null) {
            throw new DatabaseValidationException("redo checkpoint LSN must not be null");
        }
        this.repository = repository;
        this.checkpointLsn = checkpointLsn;
    }

    /**
     * 读取所有完整批次；不完整尾部由仓储停止扫描，不进入返回列表。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @return 按文件顺序排列的完整批次。
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public List<RedoLogBatch> readBatches() {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        RedoRecoveryScan scan = repository.readRecoveryScan();
        List<RedoLogBatch> all = scan.batches();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        Lsn scanEnd = scan.endLsn();
        validateRetainedRange(all, scan.retainedStartLsn());
        // 不变量：持久化 checkpoint LSN 只在对应 redo 已 durable 后才推进，故它不能领先 retained scan end；
        // torn-only ring 的 end 可由文件 header 的非零 start 提供。若 control checkpoint 仍领先，说明 redo
        // 被截断/丢失，或与 control 文件不匹配。
        // 此时继续恢复会静默跳过本应 replay 的修改、让页内容停留在 checkpoint 之前的半成品状态，按致命损坏 fail closed，
        // 不静默接受一个无法被 redo 兑现的 checkpoint。
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        if (scanEnd.value() < checkpointLsn.value()) {
            throw new RedoLogCorruptedException("persisted checkpoint LSN " + checkpointLsn.value()
                    + " exceeds retained redo scan end " + scanEnd.value()
                    + "; redo stream does not cover the checkpoint (truncated/lost redo or control mismatch)");
        }
        // scanEnd 已 >= checkpoint；torn-only ring 的 scanEnd 等于非零 retained start/checkpoint。
        recoveredToLsn = scanEnd;
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return all.stream()
                .filter(batch -> batch.range().end().value() > checkpointLsn.value())
                .toList();
    }

    /**
     * 校验仓储交付的完整批次覆盖一个连续 LSN 区间，并且 checkpoint 没有落在第一条保留批次之前。
     * 数据从 repository 的物理扫描结果进入；本方法只验证逻辑范围，不修改文件。发现 gap/overlap 或已回收
     * 所需日志时抛出致命损坏，避免 recovery 在缺失 redo 的前提下开放流量。
     */
    private void validateRetainedRange(List<RedoLogBatch> batches, Lsn retainedStartLsn) {
        long firstStart = retainedStartLsn.value();
        if (checkpointLsn.value() < firstStart) {
            throw new RedoLogCorruptedException("persisted checkpoint LSN " + checkpointLsn.value()
                    + " precedes retained redo start " + firstStart
                    + "; required redo range has been reclaimed or lost");
        }
        if (batches.isEmpty()) {
            return;
        }
        long expectedStart = firstStart;
        for (RedoLogBatch batch : batches) {
            long actualStart = batch.range().start().value();
            if (actualStart != expectedStart) {
                throw new RedoLogCorruptedException("redo batch range is not continuous: expected start "
                        + expectedStart + " but found " + actualStart + " before end "
                        + batch.range().end().value());
            }
            expectedStart = batch.range().end().value();
        }
    }

    /** 最近一次扫描的恢复终点 LSN。 */
    public Lsn recoveredToLsn() {
        return recoveredToLsn;
    }
}
