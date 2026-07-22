package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * worker 对一条 committed history log 的不可变结果。计数只描述本次实际提交的物理动作；只有 READY 允许
 * 同表后继进入 worker，也只有 dispatcher 能据 READY 执行 history finalization。
 */
record PurgeLogTaskResult(HistoryEntry entry, PurgeLogTaskStatus status,
                          int removedClustered, int removedSecondary, int skippedUnavailable,
                          DatabaseRuntimeException failure) {

    /** 校验结果身份、计数和失败状态一致性，防止调度器把损坏结果当成 table token。 */
    PurgeLogTaskResult {
        if (entry == null || status == null) {
            throw new DatabaseValidationException("purge log result entry/status must not be null");
        }
        if (removedClustered < 0 || removedSecondary < 0 || skippedUnavailable < 0) {
            throw new DatabaseValidationException("purge log result counts must be non-negative");
        }
        if ((status == PurgeLogTaskStatus.FAILED) != (failure != null)) {
            throw new DatabaseValidationException("only FAILED purge result may carry a failure");
        }
    }

    /** @return logical head 已空且可以释放同表调度依赖时为 true */
    boolean releasesTableLane() {
        return status == PurgeLogTaskStatus.READY;
    }

    /** @return FAILED 结果的领域首因；其它状态为空 */
    Optional<DatabaseRuntimeException> failureOptional() {
        return Optional.ofNullable(failure);
    }

    /**
     * 创建 logical head 已为空的成功结果。
     *
     * @param entry 当前 committed history identity
     * @param clustered 本次实际物理删除的聚簇记录数
     * @param secondary 本次实际物理删除的二级索引项数
     * @param skipped 本次因 recovery-unavailable 安全跳过的 undo 记录数
     * @return 允许同表后继执行且可由 dispatcher finalization 的 READY 结果
     */
    static PurgeLogTaskResult ready(HistoryEntry entry, int clustered, int secondary, int skipped) {
        return new PurgeLogTaskResult(entry, PurgeLogTaskStatus.READY,
                clustered, secondary, skipped, null);
    }

    /**
     * 创建 row guard busy 的延后结果；已提交的前序记录进度和计数不会回退。
     *
     * @param entry 当前 committed history identity
     * @param clustered 延后前已经完成的聚簇删除数
     * @param secondary 延后前已经完成的二级删除数
     * @param skipped 延后前已经完成的 unavailable 跳过数
     * @return 保留 history owner 且阻塞同表后继的 DEFERRED 结果
     */
    static PurgeLogTaskResult deferred(HistoryEntry entry, int clustered, int secondary, int skipped) {
        return new PurgeLogTaskResult(entry, PurgeLogTaskStatus.DEFERRED,
                clustered, secondary, skipped, null);
    }

    /**
     * 创建因同表前驱或 eligibility 复核而未执行的结果。
     *
     * @param entry 未进入记录级物理任务的 history identity
     * @return 零计数、阻塞同表 lane 的 BLOCKED 结果
     */
    static PurgeLogTaskResult blocked(HistoryEntry entry) {
        return new PurgeLogTaskResult(entry, PurgeLogTaskStatus.BLOCKED, 0, 0, 0, null);
    }

    /**
     * 创建保留短 MTR 已提交事实的领域失败结果。
     *
     * @param entry 失败日志 identity
     * @param clustered 失败前已提交的聚簇删除数
     * @param secondary 失败前已提交的二级删除数
     * @param skipped 失败前已提交的 unavailable 跳过数
     * @param failure 需要由 dispatcher 按物理顺序传播的项目领域异常
     * @return 不释放同表 lane 的 FAILED 结果
     */
    static PurgeLogTaskResult failed(HistoryEntry entry, int clustered, int secondary, int skipped,
                                     DatabaseRuntimeException failure) {
        return new PurgeLogTaskResult(entry, PurgeLogTaskStatus.FAILED,
                clustered, secondary, skipped, failure);
    }
}
