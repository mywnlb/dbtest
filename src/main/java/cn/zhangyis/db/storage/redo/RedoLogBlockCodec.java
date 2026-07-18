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
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param batch redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param firstBlockNo 参与 {@code encodeBatch} 的原始数值身份 {@code firstBlockNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code encodeBatch} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static EncodedBatch encodeBatch(RedoLogBatch batch, long firstBlockNo) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (batch == null) {
            throw new DatabaseValidationException("redo block batch must not be null");
        }
        if (batch.records().isEmpty() || batch.range().start().equals(batch.range().end())) {
            throw new DatabaseValidationException("persisted redo block batch must be non-empty");
        }
        if (firstBlockNo < 0) {
            throw new DatabaseValidationException("redo block number must not be negative: " + firstBlockNo);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        int frameBytes = frame.remaining();
        int blockCount = Math.ceilDiv(frameBytes, PAYLOAD_BYTES);
        if (firstBlockNo > Long.MAX_VALUE - blockCount) {
            throw new RedoLogCorruptedException("redo block number overflow at " + firstBlockNo);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
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
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new EncodedBatch(encoded, blockCount, firstBlockNo + blockCount);
    }

    /** 解码并完整校验单块；checksum 不匹配用专用内部异常交给 scanner 判断是否位于可容忍尾部。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param block 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code decodeBlock} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static DecodedBlock decodeBlock(byte[] block) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
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
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
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
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new DecodedBlock(blockNo, batchStartLsn, start, end, payload);
    }

    /** 计算指定前缀的 CRC32，并截断为磁盘 int。
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param length 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return 按当前持久格式计算的校验位模式；使用 Java 数值类型承载无符号结果，不代表有符号业务量
     */
    static int checksum(byte[] bytes, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    /** 已编码 batch；bytes 每次返回只读独立 view，避免调用方改变 position 影响 repository 写入。 */
    static final class EncodedBatch {
        /**
         * 本对象独占的 {@code bytes} 数据缓冲；构造和访问路径必须遵守防御性复制或受控视图约束，不能泄漏可变数组。
         */
        private final byte[] bytes;
        /**
         * 记录 {@code blockCount} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
         */
        private final int blockCount;
        /**
         * 记录 {@code nextBlockNo} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
         */
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

    /** 单块解码后的不可变 chain 片段。
     *
     * @param blockNo 参与 {@code 构造} 的原始数值身份 {@code blockNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param batchStartLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param start 范围边界是否包含起点或终点；该值只影响当前范围端点，不能改变键的排序语义
     * @param end 范围边界是否包含起点或终点；该值只影响当前范围端点，不能改变键的排序语义
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
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
