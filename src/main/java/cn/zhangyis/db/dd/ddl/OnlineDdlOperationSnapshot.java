package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * epoch标记的Online DDL弱一致只读快照。对象不持有MDL、FileChannel、page guard或可变数组；各计数单调，
 * 但不同分类允许来自相邻更新瞬间，诊断调用方不能把它当作提交裁决证据。
 *
 * @param epoch tracker每次可见更新递增的正序号
 * @param identity operation不可变身份
 * @param runtimePhase 当前进程内阶段
 * @param gatePhase 最后一次采样的table gate阶段
 * @param startedAtEpochMillis tracker注册时间
 * @param updatedAtEpochMillis 最后一次投影更新时间
 * @param waitReason 当前主要等待原因
 * @param durablePhase 最后观察到的durable marker phase；prepare前为空
 * @param controlState 最后观察到的durable control
 * @param rowLogGeneration 最后观察到的row-log generation；未创建为0
 * @param cancellationReason durable/user abort原因；不存在为空
 * @param rowsScanned 已完成base scan行数
 * @param batchesScanned 已完成base scan批次数
 * @param estimatedRows 可选statistics估计；未知必须为空
 * @param lastClusteredKeyDigest 最后continuation key的安全摘要，不得保存原始业务值
 * @param candidateCount 已追加candidate frame数
 * @param rowLogBytes 当前row-log文件字节数
 * @param maxRowLogBytes 配置总上限；未知为0
 * @param abortReserveBytes terminal abort保留字节；未知为0
 * @param highestAppendedSequence row-log已追加高水位
 * @param highestForcedSequence row-log已force高水位，不得超过appended
 * @param inFlightAdmissions gate内短admission计数
 * @param ioLeases gate内row-log I/O lease计数
 * @param terminalRedoHighWater final cutover等待的redo LSN高水位
 * @param cancelCapable marker protocol是否允许durable cancel CAS
 * @param retirementFencePresent marker是否已安装旧资源回收边界
 * @param retirementSafe 最后一次barrier采样是否已允许物理回收
 * @param terminalResult 本进程最终结果；运行中为NONE
 * @param lastErrorCode 有界领域错误码；不保存无界异常message/stack
 * @param forwardRecoveryRequired 是否已经越过forward fence但当前进程未完整收敛
 */
public record OnlineDdlOperationSnapshot(
        long epoch,
        OnlineDdlOperationIdentity identity,
        OnlineDdlRuntimePhase runtimePhase,
        OnlineDdlTablePhase gatePhase,
        long startedAtEpochMillis,
        long updatedAtEpochMillis,
        OnlineDdlWaitReason waitReason,
        Optional<DdlLogPhase> durablePhase,
        DdlControlState controlState,
        long rowLogGeneration,
        Optional<DdlCancellationReason> cancellationReason,
        long rowsScanned,
        long batchesScanned,
        OptionalLong estimatedRows,
        Optional<String> lastClusteredKeyDigest,
        long candidateCount,
        long rowLogBytes,
        long maxRowLogBytes,
        int abortReserveBytes,
        long highestAppendedSequence,
        long highestForcedSequence,
        long inFlightAdmissions,
        long ioLeases,
        long terminalRedoHighWater,
        boolean cancelCapable,
        boolean retirementFencePresent,
        boolean retirementSafe,
        OnlineDdlTerminalResult terminalResult,
        Optional<String> lastErrorCode,
        boolean forwardRecoveryRequired) {

    public OnlineDdlOperationSnapshot {
        if (epoch <= 0 || identity == null || runtimePhase == null || gatePhase == null
                || startedAtEpochMillis < 0 || updatedAtEpochMillis < startedAtEpochMillis
                || waitReason == null || durablePhase == null || controlState == null
                || rowLogGeneration < 0 || cancellationReason == null
                || rowsScanned < 0 || batchesScanned < 0 || estimatedRows == null
                || estimatedRows.isPresent() && estimatedRows.orElseThrow() < 0
                || lastClusteredKeyDigest == null || candidateCount < 0 || rowLogBytes < 0
                || maxRowLogBytes < 0 || abortReserveBytes < 0
                || maxRowLogBytes > 0 && abortReserveBytes >= maxRowLogBytes
                || highestAppendedSequence < 0 || highestForcedSequence < 0
                || highestForcedSequence > highestAppendedSequence
                || inFlightAdmissions < 0 || ioLeases < 0 || terminalRedoHighWater < 0
                || terminalResult == null || lastErrorCode == null) {
            throw new DatabaseValidationException("invalid Online DDL operation snapshot");
        }
        lastClusteredKeyDigest.ifPresent(value -> requireBounded(value, 128, "clustered key digest"));
        lastErrorCode.ifPresent(value -> requireBounded(value, 64, "error code"));
    }

    private static void requireBounded(String value, int limit, String field) {
        if (value.isBlank() || value.length() > limit) {
            throw new DatabaseValidationException(
                    "Online DDL " + field + " is blank or exceeds " + limit);
        }
    }

    /** @return runtime或terminal状态是否已经形成不可再运行的本进程终点。 */
    public boolean terminal() {
        return terminalResult != OnlineDdlTerminalResult.NONE;
    }
}
