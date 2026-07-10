package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.List;

/**
 * 一段连续 LogBlock 扫描的不可变结果。
 *
 * @param batches 完整且两级 checksum 均通过的 batch。
 * @param validBytes 可安全保留的完整 block 字节数；未闭合 chain 不计入。
 * @param nextBlockNo 后续 append 可使用的 block 编号；torn chain 的编号允许安全复用。
 * @param endLsn 最后完整 batch 的逻辑结束 LSN；无 batch 时等于扫描请求的起始 LSN。
 * @param tornTail 是否在允许的位置发现并丢弃了物理尾部或未闭合 chain。
 */
record RedoLogBlockScanResult(List<RedoLogBatch> batches, int validBytes,
                              long nextBlockNo, Lsn endLsn, boolean tornTail) {

    RedoLogBlockScanResult {
        if (batches == null || endLsn == null) {
            throw new DatabaseValidationException("redo block scan result fields must not be null");
        }
        if (validBytes < 0 || validBytes % RedoLogBlockCodec.BLOCK_BYTES != 0 || nextBlockNo < 0) {
            throw new DatabaseValidationException("redo block scan result boundary is invalid");
        }
        batches = List.copyOf(batches);
    }
}
