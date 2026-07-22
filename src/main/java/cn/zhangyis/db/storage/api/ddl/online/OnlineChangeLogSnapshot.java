package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Online change-log 在单次文件状态锁内复制的不可变诊断快照。该对象不持有 FileChannel、
 * 可变 frame 数组或锁，不能被当作 recovery 裁决证据。
 *
 * @param generation 当前 capture generation，始终为正且 reset 后单调递增
 * @param candidateCount 当前 generation 已完整追加的 candidate frame 数
 * @param sizeBytes header 与全部完整 frame 的当前总字节数
 * @param maxBytes 该文件的固定容量上限
 * @param terminalReserveBytes candidate 不可占用的终止证据预留字节数
 * @param highestAppendedSequence 已完整写入 channel 的最大序号
 * @param highestForcedSequence 已由成功 force 覆盖的最大序号
 * @param abortRequired 是否已持久观察到 ABORT_REQUIRED
 * @param failed 文件 I/O 结果未知后是否已 fail-stop
 * @param closed 当前进程是否已关闭该句柄
 * @param capturing 当前 generation 是否已进入 candidate 可追加区间
 * @param sealed 当前 generation 是否已封闭 candidate 追加
 * @param reconciled 当前 generation 是否已持久完成 reconciliation
 */
public record OnlineChangeLogSnapshot(
        long generation,
        long candidateCount,
        long sizeBytes,
        long maxBytes,
        int terminalReserveBytes,
        long highestAppendedSequence,
        long highestForcedSequence,
        boolean abortRequired,
        boolean failed,
        boolean closed,
        boolean capturing,
        boolean sealed,
        boolean reconciled) {

    public OnlineChangeLogSnapshot {
        if (generation <= 0 || candidateCount < 0 || sizeBytes < 0
                || maxBytes <= 0 || sizeBytes > maxBytes
                || terminalReserveBytes < OnlineIndexChangeLog.MIN_TERMINAL_RESERVE_BYTES
                || terminalReserveBytes >= maxBytes
                || highestAppendedSequence < 0 || highestForcedSequence < 0
                || highestForcedSequence > highestAppendedSequence
                || sealed && !capturing || reconciled && !sealed
                || abortRequired && reconciled) {
            throw new DatabaseValidationException("invalid Online DDL change-log snapshot");
        }
    }
}
