package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * 连续 LogBlock 的 batch-chain 扫描器。它只允许调用方指定的逻辑末文件容忍 torn tail；一旦完整 block 的
 * checksum 已通过，任何字段或内层 frame 异常都保持致命，不能用“最后一块”掩盖真实数据损坏。
 */
final class RedoLogBlockScanner {

    private RedoLogBlockScanner() {
    }

    /**
     * 从缓冲起点扫描完整 batch。expectedFirstBlockNo 为空只允许首个保留 ring 文件自行声明编号；之后仍严格连续。
     * expectedStartLsn 是文件头/单文件约定的逻辑起点，无完整 batch 时也作为结果 endLsn。
     */
    static RedoLogBlockScanResult scan(ByteBuffer content, boolean allowTornTail,
                                       OptionalLong expectedFirstBlockNo, long expectedStartLsn) {
        if (content == null || expectedFirstBlockNo == null) {
            throw new DatabaseValidationException("redo log block scan inputs must not be null");
        }
        if (expectedStartLsn < 0 || (expectedFirstBlockNo.isPresent() && expectedFirstBlockNo.getAsLong() < 0)) {
            throw new DatabaseValidationException("redo log block scan boundary must not be negative");
        }
        ByteBuffer input = content.duplicate();
        int origin = input.position();
        List<RedoLogBatch> batches = new ArrayList<>();
        long nextBlockNo = expectedFirstBlockNo.orElse(0L);
        long nextLsn = expectedStartLsn;
        int validBytes = 0;
        boolean tornTail = false;
        Assembly assembly = null;

        while (input.hasRemaining()) {
            int blockOffset = input.position() - origin;
            if (input.remaining() >= Integer.BYTES
                    && input.getInt(input.position()) == RedoBatchFrameCodec.MAGIC) {
                throw new RedoLogFormatException("legacy raw RLG1 redo is not supported by LogBlock v1");
            }
            if (input.remaining() < RedoLogBlockCodec.BLOCK_BYTES) {
                if (!allowTornTail) {
                    throw new RedoLogCorruptedException(
                            "short redo log block before logical tail at byte " + blockOffset);
                }
                tornTail = true;
                break;
            }
            byte[] blockBytes = new byte[RedoLogBlockCodec.BLOCK_BYTES];
            input.get(blockBytes);
            boolean physicalLast = !input.hasRemaining();
            RedoLogBlockCodec.DecodedBlock block;
            try {
                block = RedoLogBlockCodec.decodeBlock(blockBytes);
            } catch (RedoLogBlockCodec.PhysicalBlockCorruption physical) {
                if (allowTornTail && physicalLast) {
                    tornTail = true;
                    break;
                }
                throw physical;
            }

            if (expectedFirstBlockNo.isEmpty() && batches.isEmpty() && assembly == null) {
                nextBlockNo = block.blockNo();
            }
            if (block.blockNo() != nextBlockNo) {
                throw new RedoLogCorruptedException("redo log block number discontinuity: expected="
                        + nextBlockNo + ", actual=" + block.blockNo());
            }
            if (assembly == null) {
                if (!block.start()) {
                    throw new RedoLogCorruptedException(
                            "redo log block continuation appears without START at block " + block.blockNo());
                }
                if (block.batchStartLsn() != nextLsn) {
                    throw new RedoLogCorruptedException("redo batch start LSN discontinuity: expected="
                            + nextLsn + ", actual=" + block.batchStartLsn());
                }
                assembly = new Assembly(block.blockNo(), block.batchStartLsn());
            } else {
                if (block.start() || block.batchStartLsn() != assembly.batchStartLsn) {
                    throw new RedoLogCorruptedException("redo batch chain changed START/LSN at block "
                            + block.blockNo());
                }
            }
            assembly.payload.writeBytes(block.payload());
            nextBlockNo = Math.addExact(block.blockNo(), 1L);

            if (block.end()) {
                RedoLogBatch batch = decodeCompleteBatch(assembly);
                if (batch.range().start().value() != nextLsn) {
                    throw new RedoLogCorruptedException("decoded redo batch range is discontinuous at LSN "
                            + batch.range().start().value());
                }
                batches.add(batch);
                nextLsn = batch.range().end().value();
                validBytes = blockOffset + RedoLogBlockCodec.BLOCK_BYTES;
                assembly = null;
            }
        }

        if (assembly != null) {
            if (!allowTornTail) {
                throw new RedoLogCorruptedException("redo batch chain is not closed before file boundary: block="
                        + assembly.firstBlockNo);
            }
            tornTail = true;
            nextBlockNo = assembly.firstBlockNo;
        }
        return new RedoLogBlockScanResult(
                batches, validBytes, nextBlockNo, Lsn.of(nextLsn), tornTail);
    }

    /** END block 已声明 frame 完整，因此 decode 停在尾部或解析出非一批都属于致命语义损坏。 */
    private static RedoLogBatch decodeCompleteBatch(Assembly assembly) {
        ByteBuffer frame = ByteBuffer.wrap(assembly.payload.toByteArray());
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);
        if (decoded.size() != 1 || frame.hasRemaining()) {
            throw new RedoLogCorruptedException("redo LogBlock END does not contain exactly one complete batch");
        }
        RedoLogBatch batch = decoded.getFirst();
        if (batch.range().start().value() != assembly.batchStartLsn) {
            throw new RedoLogCorruptedException("redo LogBlock batchStartLsn does not match inner frame: block="
                    + assembly.batchStartLsn + ", frame=" + batch.range().start().value());
        }
        return batch;
    }

    /** 当前未闭合 batch 的线程局部组装状态；scanner 无共享可变状态。 */
    private static final class Assembly {
        private final long firstBlockNo;
        private final long batchStartLsn;
        private final ByteArrayOutputStream payload = new ByteArrayOutputStream();

        private Assembly(long firstBlockNo, long batchStartLsn) {
            this.firstBlockNo = firstBlockNo;
            this.batchStartLsn = batchStartLsn;
        }
    }
}
