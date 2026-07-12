package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * LogBlock v1 的公开纯尺寸公式。该类不编码伪批次、不读取 repository 状态，只把逻辑 record 字节换算成
 * {@code RLG1 frame + 512B LogBlock chain} 的物理上界，供上层在获取页资源前完成准入。
 */
public final class RedoLogBlockSizing {

    /** 可被当前 frame codec 恢复的单批最大逻辑 record 字节数。 */
    public static final long MAX_LOGICAL_BATCH_BYTES =
            RedoBatchFrameCodec.MAX_PAYLOAD_BYTES - RedoBatchFrameCodec.PAYLOAD_HEADER_BYTES;

    private RedoLogBlockSizing() {
    }

    /**
     * 计算逻辑 record 字节对应的完整 block chain 字节数。零记录不会落盘，故返回零；正值公式为
     * {@code ceil((frameHeader + payloadHeader + logical)/blockPayload) * blockBytes}。
     */
    public static long physicalBytesForLogical(long logicalBytes) {
        if (logicalBytes < 0 || logicalBytes > MAX_LOGICAL_BATCH_BYTES) {
            throw new DatabaseValidationException("redo logical batch bytes outside recoverable range: "
                    + logicalBytes + " (max=" + MAX_LOGICAL_BATCH_BYTES + ")");
        }
        if (logicalBytes == 0) {
            return 0;
        }
        long frameBytes = Math.addExact(
                RedoBatchFrameCodec.FRAME_HEADER_BYTES + RedoBatchFrameCodec.PAYLOAD_HEADER_BYTES,
                logicalBytes);
        long blockCount = Math.ceilDiv(frameBytes, RedoLogBlockCodec.PAYLOAD_BYTES);
        return Math.multiplyExact(blockCount, RedoLogBlockCodec.BLOCK_BYTES);
    }
}
