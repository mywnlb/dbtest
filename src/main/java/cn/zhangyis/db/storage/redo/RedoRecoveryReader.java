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

    public RedoRecoveryReader(RedoLogFileRepository repository) {
        this(repository, Lsn.of(0));
    }

    /**
     * 创建从指定 checkpoint 后恢复的 reader。reader 从 repository retained 边界顺序扫描完整 block chain，但只返回
     * {@code batch.endLsn > checkpointLsn} 的批次；若 checkpoint 落在某个批次内部，则保留该批次并交给 pageLSN 幂等判断。
     *
     * @param repository redo 文件仓储。
     * @param checkpointLsn 持久化 checkpoint label 中的恢复起点。
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
     * @return 按文件顺序排列的完整批次。
     */
    public List<RedoLogBatch> readBatches() {
        RedoRecoveryScan scan = repository.readRecoveryScan();
        List<RedoLogBatch> all = scan.batches();
        Lsn scanEnd = scan.endLsn();
        validateRetainedRange(all, scan.retainedStartLsn());
        // 不变量：持久化 checkpoint LSN 只在对应 redo 已 durable 后才推进，故它不能领先 retained scan end；
        // torn-only ring 的 end 可由文件 header 的非零 start 提供。若 control checkpoint 仍领先，说明 redo
        // 被截断/丢失，或与 control 文件不匹配。
        // 此时继续恢复会静默跳过本应 replay 的修改、让页内容停留在 checkpoint 之前的半成品状态，按致命损坏 fail closed，
        // 不静默接受一个无法被 redo 兑现的 checkpoint。
        if (scanEnd.value() < checkpointLsn.value()) {
            throw new RedoLogCorruptedException("persisted checkpoint LSN " + checkpointLsn.value()
                    + " exceeds retained redo scan end " + scanEnd.value()
                    + "; redo stream does not cover the checkpoint (truncated/lost redo or control mismatch)");
        }
        // scanEnd 已 >= checkpoint；torn-only ring 的 scanEnd 等于非零 retained start/checkpoint。
        recoveredToLsn = scanEnd;
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
