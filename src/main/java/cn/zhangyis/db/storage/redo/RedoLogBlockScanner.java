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
     *
     * @param content 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param allowTornTail 恢复容错策略标志；只允许在契约明确的损坏或结果不确定场景放宽校验，不得掩盖其他数据损坏
     * @param expectedFirstBlockNo 可选的 {@code expectedFirstBlockNo}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param expectedStartLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @return {@code scan} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
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

    /** END block 已声明 frame 完整，因此 decode 停在尾部或解析出非一批都属于致命语义损坏。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param assembly 当前算法已准备的中间状态；不得为 {@code null}，必须由本次扫描、日志组装或事务终结流程创建且尚未发布
     * @return {@code decodeCompleteBatch} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static RedoLogBatch decodeCompleteBatch(Assembly assembly) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        ByteBuffer frame = ByteBuffer.wrap(assembly.payload.toByteArray());
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        List<RedoLogBatch> decoded = RedoBatchFrameCodec.decodeFrames(frame);
        if (decoded.size() != 1 || frame.hasRemaining()) {
            throw new RedoLogCorruptedException("redo LogBlock END does not contain exactly one complete batch");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        RedoLogBatch batch = decoded.getFirst();
        if (batch.range().start().value() != assembly.batchStartLsn) {
            throw new RedoLogCorruptedException("redo LogBlock batchStartLsn does not match inner frame: block="
                    + assembly.batchStartLsn + ", frame=" + batch.range().start().value());
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return batch;
    }

    /** 当前未闭合 batch 的线程局部组装状态；scanner 无共享可变状态。 */
    private static final class Assembly {
        /**
         * 记录 {@code firstBlockNo} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
         */
        private final long firstBlockNo;
        /**
         * 记录 {@code batchStartLsn} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private final long batchStartLsn;
        /**
         * 本对象持有的 {@code payload} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
         */
        private final ByteArrayOutputStream payload = new ByteArrayOutputStream();

        private Assembly(long firstBlockNo, long batchStartLsn) {
            this.firstBlockNo = firstBlockNo;
            this.batchStartLsn = batchStartLsn;
        }
    }
}
