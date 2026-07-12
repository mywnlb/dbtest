package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一批已冻结 redo records 的精确尺寸。
 *
 * @param logicalBytes {@code Σ RedoRecord.byteLength()}，也是该批占用的逻辑 LSN 长度。
 * @param physicalBlockBytes LogBlock v1 编码后占用的 512B block 总字节数。
 */
public record RedoAppendUsage(long logicalBytes, long physicalBlockBytes) {

    public RedoAppendUsage {
        if (logicalBytes < 0 || physicalBlockBytes < 0) {
            throw new DatabaseValidationException("redo append usage must not be negative: logical="
                    + logicalBytes + ", physical=" + physicalBlockBytes);
        }
    }
}
