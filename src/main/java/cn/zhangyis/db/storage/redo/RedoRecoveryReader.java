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
     * 创建从指定 checkpoint 后恢复的 reader。reader 仍从文件头顺序扫描完整 frame，但只返回
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
        List<RedoLogBatch> all = repository.readBatches();
        Lsn lastCompleteEnd = all.isEmpty() ? Lsn.of(0) : all.get(all.size() - 1).range().end();
        // 不变量：持久化 checkpoint LSN 只在对应 redo 已 durable 后才推进，故它必然落在已扫描到的完整 redo 之内。
        // 若 control 文件 checkpoint 领先于最后一条完整 redo 批次，说明 redo 被截断/丢失，或与 control 文件不匹配。
        // 此时继续恢复会静默跳过本应 replay 的修改、让页内容停留在 checkpoint 之前的半成品状态，按致命损坏 fail closed，
        // 不静默接受一个无法被 redo 兑现的 checkpoint。
        if (lastCompleteEnd.value() < checkpointLsn.value()) {
            throw new RedoLogCorruptedException("persisted checkpoint LSN " + checkpointLsn.value()
                    + " exceeds last complete redo batch end " + lastCompleteEnd.value()
                    + "; redo stream does not cover the checkpoint (truncated/lost redo or control mismatch)");
        }
        // 非空时 lastCompleteEnd 已 >= checkpointLsn（上面的不变量保证），等价于旧实现的 max(checkpointLsn, lastEnd)。
        recoveredToLsn = all.isEmpty() ? checkpointLsn : lastCompleteEnd;
        return all.stream()
                .filter(batch -> batch.range().end().value() > checkpointLsn.value())
                .toList();
    }

    /** 最近一次扫描的恢复终点 LSN。 */
    public Lsn recoveredToLsn() {
        return recoveredToLsn;
    }
}
