package cn.zhangyis.db.storage.api.ddl.online;

import java.time.Duration;
import java.util.List;

/**
 * 通用Online ALTER journal稳定端口。与旧单索引端口分离，以便独立演进manifest和READY_TO_PUBLISH状态，
 * 同时复用DML admission所需的candidate/force/abort最小协议。
 */
public interface OnlineAlterChangeLog extends OnlineDdlChangeLog {

    /** candidate与终止frame之间必须永久保留的格式演进空间。 */
    int MIN_TERMINAL_RESERVE_BYTES = 256;

    /** @return offset 0已经force的不可变owner header。 */
    OnlineAlterLogHeader header();

    /**
     * 追加由coordinator拥有的状态frame；candidate、watermark和abort不能走此入口。
     *
     * @param type generation/capturing/sealed/ready/reconciled之一
     * @param payload 当前v1要求为空的状态payload
     * @return 已完整append但不保证force的新sequence
     */
    long appendState(OnlineAlterLogRecordType type, byte[] payload);

    /** @return 当前generation最后完整append的sequence；仅header时为0。 */
    long highestAppendedSequence();

    /** @return 已由force覆盖的frame high-water。 */
    long highestForcedSequence();

    /** @return 当前header与完整frame总字节数。 */
    long sizeBytes();

    /** @return 在单次状态锁内复制的有界诊断快照。 */
    OnlineChangeLogSnapshot snapshot();

    /** @return 已校验frame的不可变内存快照；仅用于有界测试和小型协调步骤。 */
    List<OnlineAlterLogRecord> readAll();

    /**
     * 从给定sequence之后流式复制至多limit条candidate；state/watermark frame不计入返回上限。
     *
     * @param sequenceExclusive 上一批最后candidate sequence；首批传0
     * @param limit 正批次上限，调用方不得用日志容量作为一次heap分配大小
     * @return 按sequence递增的candidate快照；空列表表示其后已无candidate
     */
    List<OnlineAlterLogRecord> readCandidatesAfter(long sequenceExclusive, int limit);

    /**
     * recovery关闭流量期间清除旧generation frame并force，仅保留immutable manifest。
     *
     * @param timeout 正等待预算
     */
    void resetToManifest(Duration timeout);
}
