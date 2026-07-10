package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 固定 512B LogBlock v1 的纯编解码器。它只负责单个 block 的字段、padding、trailer 和 checksum，
 * batch chain、torn-tail 位置与跨文件连续性由 {@link RedoLogBlockScanner} 和 repository 判断。
 */
final class RedoLogBlockCodec {

    /** ASCII "RLB1"，位于每个物理 block 的首 4 字节。 */
    static final int MAGIC = 0x524C4231;
    /** 当前 LogBlock 持久格式版本。 */
    static final int FORMAT_VERSION = 1;
    /** 固定 block 大小。 */
    static final int BLOCK_BYTES = 512;
    /** header 固定长度。 */
    static final int HEADER_BYTES = 32;
    /** 每块可承载的 batch-frame 字节数。 */
    static final int PAYLOAD_BYTES = 472;
    /** trailer blockNo 镜像偏移。 */
    static final int TRAILER_BLOCK_NO_OFFSET = 504;
    /** checksum 字段偏移；CRC 覆盖此前全部 508B。 */
    static final int CHECKSUM_OFFSET = 508;
    /** batch chain 首块。 */
    static final int FLAG_START = 1;
    /** batch chain 尾块。 */
    static final int FLAG_END = 1 << 1;
    /** 后续 checkpoint marker 位；v1 必须为 0。 */
    static final int FLAG_CHECKPOINT_MARKER = 1 << 2;
    /** 续块没有新的 record 起点。 */
    static final int NO_RECORD_OFFSET = 0xFFFF;
    /** 首个 record 位于 RLG1 frame header 与 payload header 之后。 */
    static final int FIRST_RECORD_OFFSET =
            RedoBatchFrameCodec.FRAME_HEADER_BYTES + RedoBatchFrameCodec.PAYLOAD_HEADER_BYTES;

    private RedoLogBlockCodec() {
    }

    /**
     * 把一个非空 MTR batch 编码为独立、完整封口的 block chain。blockNo 在成功计算完整 chain 后才返回，
     * 调用方可以在文件写失败时保留原 nextBlockNo，后续覆盖 torn tail。
     */
    static EncodedBatch encodeBatch(RedoLogBatch batch, long firstBlockNo) {
        if (batch == null) {
            throw new DatabaseValidationException("redo block batch must not be null");
        }
        if (batch.records().isEmpty() || batch.range().start().equals(batch.range().end())) {
            throw new DatabaseValidationException("persisted redo block batch must be non-empty");
        }
        if (firstBlockNo < 0) {
            throw new DatabaseValidationException("redo block number must not be negative: " + firstBlockNo);
        }
        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        int frameBytes = frame.remaining();
        int blockCount = Math.ceilDiv(frameBytes, PAYLOAD_BYTES);
        if (firstBlockNo > Long.MAX_VALUE - blockCount) {
            throw new RedoLogCorruptedException("redo block number overflow at " + firstBlockNo);
        }
        byte[] encoded = new byte[Math.multiplyExact(blockCount, BLOCK_BYTES)];
        ByteBuffer output = ByteBuffer.wrap(encoded);
        for (int i = 0; i < blockCount; i++) {
            int dataLength = Math.min(PAYLOAD_BYTES, frame.remaining());
            int flags = (i == 0 ? FLAG_START : 0) | (i == blockCount - 1 ? FLAG_END : 0);
            byte[] block = new byte[BLOCK_BYTES];
            ByteBuffer view = ByteBuffer.wrap(block);
            long blockNo = firstBlockNo + i;
            view.putInt(MAGIC);
            view.putInt(FORMAT_VERSION);
            view.putLong(blockNo);
            view.putLong(batch.range().start().value());
            view.putShort((short) dataLength);
            view.putShort((short) (i == 0 ? FIRST_RECORD_OFFSET : NO_RECORD_OFFSET));
            view.putInt(flags);
            byte[] payload = new byte[dataLength];
            frame.get(payload);
            view.put(payload);
            view.putInt(TRAILER_BLOCK_NO_OFFSET, (int) blockNo);
            view.putInt(CHECKSUM_OFFSET, checksum(block, CHECKSUM_OFFSET));
            output.put(block);
        }
        output.flip();
        return new EncodedBatch(encoded, blockCount, firstBlockNo + blockCount);
    }

    /** 解码并完整校验单块；checksum 不匹配用专用内部异常交给 scanner 判断是否位于可容忍尾部。 */
    static DecodedBlock decodeBlock(byte[] block) {
        if (block == null || block.length != BLOCK_BYTES) {
            throw new DatabaseValidationException("redo log block must contain exactly " + BLOCK_BYTES + " bytes");
        }
        ByteBuffer view = ByteBuffer.wrap(block);
        int magic = view.getInt(0);
        if (magic == RedoBatchFrameCodec.MAGIC) {
            throw new RedoLogFormatException("legacy raw RLG1 redo is not supported by LogBlock v1");
        }
        int expectedChecksum = view.getInt(CHECKSUM_OFFSET);
        if (expectedChecksum != checksum(block, CHECKSUM_OFFSET)) {
            throw new PhysicalBlockCorruption("redo log block checksum mismatch");
        }
        if (magic != MAGIC) {
            throw new RedoLogCorruptedException("redo log block magic mismatch: " + magic);
        }
        int version = view.getInt(4);
        if (version != FORMAT_VERSION) {
            throw new RedoLogFormatException("unsupported redo log block format version: " + version);
        }
        long blockNo = view.getLong(8);
        long batchStartLsn = view.getLong(16);
        int dataLength = Short.toUnsignedInt(view.getShort(24));
        int firstRecordOffset = Short.toUnsignedInt(view.getShort(26));
        int flags = view.getInt(28);
        if (blockNo < 0 || blockNo == Long.MAX_VALUE || batchStartLsn < 0) {
            throw new RedoLogCorruptedException("redo log block number/LSN is invalid: block="
                    + blockNo + ", startLsn=" + batchStartLsn);
        }
        if ((flags & FLAG_CHECKPOINT_MARKER) != 0) {
            throw new RedoLogCorruptedException("redo checkpoint marker flag is reserved in LogBlock v1");
        }
        if ((flags & ~(FLAG_START | FLAG_END | FLAG_CHECKPOINT_MARKER)) != 0) {
            throw new RedoLogCorruptedException("redo log block flags are invalid in v1: " + flags);
        }
        boolean start = (flags & FLAG_START) != 0;
        boolean end = (flags & FLAG_END) != 0;
        if (dataLength <= 0 || dataLength > PAYLOAD_BYTES || (!end && dataLength != PAYLOAD_BYTES)) {
            throw new RedoLogCorruptedException("redo log block data length is invalid: " + dataLength);
        }
        int expectedRecordOffset = start ? FIRST_RECORD_OFFSET : NO_RECORD_OFFSET;
        if (firstRecordOffset != expectedRecordOffset) {
            throw new RedoLogCorruptedException("redo log block first record offset is invalid: "
                    + firstRecordOffset + ", expected=" + expectedRecordOffset);
        }
        if (view.getInt(TRAILER_BLOCK_NO_OFFSET) != (int) blockNo) {
            throw new RedoLogCorruptedException("redo log block trailer number mismatch: " + blockNo);
        }
        int payloadEnd = HEADER_BYTES + dataLength;
        for (int i = payloadEnd; i < TRAILER_BLOCK_NO_OFFSET; i++) {
            if (block[i] != 0) {
                throw new RedoLogCorruptedException("redo log block padding is not zero at offset " + i);
            }
        }
        byte[] payload = new byte[dataLength];
        System.arraycopy(block, HEADER_BYTES, payload, 0, dataLength);
        return new DecodedBlock(blockNo, batchStartLsn, start, end, payload);
    }

    /** 计算指定前缀的 CRC32，并截断为磁盘 int。 */
    static int checksum(byte[] bytes, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    /** 已编码 batch；bytes 每次返回只读独立 view，避免调用方改变 position 影响 repository 写入。 */
    static final class EncodedBatch {
        private final byte[] bytes;
        private final int blockCount;
        private final long nextBlockNo;

        private EncodedBatch(byte[] bytes, int blockCount, long nextBlockNo) {
            this.bytes = bytes;
            this.blockCount = blockCount;
            this.nextBlockNo = nextBlockNo;
        }

        ByteBuffer bytes() {
            return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
        }

        int blockCount() {
            return blockCount;
        }

        long nextBlockNo() {
            return nextBlockNo;
        }

        int byteLength() {
            return bytes.length;
        }
    }

    /** 单块解码后的不可变 chain 片段。 */
    record DecodedBlock(long blockNo, long batchStartLsn, boolean start, boolean end, byte[] payload) {
        DecodedBlock {
            payload = payload.clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    /** 只有物理 checksum 失败可由 scanner 在逻辑末块降级为 torn tail。 */
    static final class PhysicalBlockCorruption extends RedoLogCorruptedException {
        private PhysicalBlockCorruption(String message) {
            super(message);
        }
    }
}
