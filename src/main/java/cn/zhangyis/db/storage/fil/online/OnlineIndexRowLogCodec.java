package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecordType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.zip.CRC32C;

/**
 * Online index row-log v1 的稳定二进制 codec。它只解释文件 framing、identity、摘要和CRC，不理解DD manifest
 * 或 candidate 中的 record 语义。
 */
public final class OnlineIndexRowLogCodec {

    /** ASCII "OIDXLOG1"，用于拒绝把其它sidecar误当online row log。 */
    private static final long HEADER_MAGIC = 0x4F4944584C4F4731L;
    /** header/frame 当前写版本。 */
    private static final int FORMAT_VERSION = 1;
    /** magic/version/headerLength，文件open先读取该固定前缀再分配完整header。 */
    static final int HEADER_PREFIX_BYTES = Long.BYTES + Integer.BYTES * 2;
    /** manifest 防内存放大的v1上限；实际DD manifest远小于此值。 */
    private static final int MAX_MANIFEST_BYTES = 1024 * 1024;
    /** 单candidate编码上限；二级entry仍受页内record上限约束，此处只防损坏长度分配。 */
    static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;
    /** frame中除payload外的总字节数。 */
    static final int MIN_FRAME_BYTES = Integer.BYTES + Short.BYTES * 2 + Long.BYTES * 3
            + Integer.BYTES * 2;
    /** digest使用固定SHA-256。 */
    private static final int DIGEST_BYTES = 32;

    /**
     * 编码immutable header。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>复制manifest并计算SHA-256，避免调用方并发修改输入。</li>
     *     <li>写入magic/version/总长和全部owner/version identity。</li>
     *     <li>写入manifest长度、摘要和payload。</li>
     *     <li>对之前全部字节计算CRC32C并追加，返回独立byte数组。</li>
     * </ol>
     *
     * @param header 已通过领域构造校验的immutable header
     * @return 可从文件offset 0完整写入的稳定v1字节
     * @throws DatabaseValidationException manifest超过格式上限或摘要算法不可用时抛出
     */
    public byte[] encodeHeader(OnlineIndexLogHeader header) {
        // 1、冻结opaque manifest并计算跨DD/storage边界的稳定摘要。
        if (header == null) {
            throw new DatabaseValidationException("online index log header must not be null");
        }
        byte[] manifest = header.manifest();
        if (manifest.length > MAX_MANIFEST_BYTES) {
            throw new DatabaseValidationException("online index manifest exceeds format limit: " + manifest.length);
        }
        byte[] digest = sha256(manifest);
        int headerLength = HEADER_PREFIX_BYTES + Long.BYTES * 6 + Integer.BYTES
                + DIGEST_BYTES + manifest.length + Integer.BYTES;
        ByteBuffer encoded = ByteBuffer.allocate(headerLength).order(ByteOrder.BIG_ENDIAN);

        // 2、identity重复进入DDL marker/page3交叉校验，不能从文件名反向猜测。
        encoded.putLong(HEADER_MAGIC).putInt(FORMAT_VERSION).putInt(headerLength)
                .putLong(header.buildId().value()).putLong(header.tableId()).putLong(header.indexId())
                .putLong(header.sourceDictionaryVersion()).putLong(header.targetDictionaryVersion())
                .putLong(header.rowFormatVersion());

        // 3、manifest本身由DD codec解释；storage只保留长度和摘要。
        encoded.putInt(manifest.length).put(digest).put(manifest);

        // 4、header CRC覆盖magic到manifest最后一字节，避免identity与payload单侧损坏。
        encoded.putInt(crc32c(encoded.array(), 0, encoded.position()));
        return encoded.array();
    }

    /**
     * 解码并完整校验header。
     *
     * @param source position指向header起点、limit恰好覆盖完整header的buffer
     * @return 防御性复制manifest的immutable领域对象
     * @throws DatabaseValidationException magic/version/长度/摘要/CRC/identity非法时抛出
     */
    public OnlineIndexLogHeader decodeHeader(ByteBuffer source) {
        if (source == null) {
            throw new DatabaseValidationException("online index header source must not be null");
        }
        ByteBuffer input = source.slice().order(ByteOrder.BIG_ENDIAN);
        if (input.remaining() < HEADER_PREFIX_BYTES + Long.BYTES * 6 + Integer.BYTES
                + DIGEST_BYTES + Integer.BYTES) {
            throw new DatabaseValidationException("online index log header is truncated");
        }
        int totalBytes = input.remaining();
        long magic = input.getLong();
        int version = input.getInt();
        int declaredLength = input.getInt();
        if (magic != HEADER_MAGIC || version != FORMAT_VERSION || declaredLength != totalBytes) {
            throw new DatabaseValidationException("online index log header magic/version/length mismatch");
        }
        long buildId = input.getLong();
        long tableId = input.getLong();
        long indexId = input.getLong();
        long sourceVersion = input.getLong();
        long targetVersion = input.getLong();
        long rowFormatVersion = input.getLong();
        int manifestLength = input.getInt();
        if (manifestLength <= 0 || manifestLength > MAX_MANIFEST_BYTES
                || input.remaining() != DIGEST_BYTES + manifestLength + Integer.BYTES) {
            throw new DatabaseValidationException("online index log manifest length is invalid");
        }
        byte[] expectedDigest = new byte[DIGEST_BYTES];
        input.get(expectedDigest);
        byte[] manifest = new byte[manifestLength];
        input.get(manifest);
        int expectedCrc = input.getInt();
        ByteBuffer original = source.slice();
        byte[] bytes = new byte[original.remaining()];
        original.get(bytes);
        int actualCrc = crc32c(bytes, 0, bytes.length - Integer.BYTES);
        if (expectedCrc != actualCrc || !Arrays.equals(expectedDigest, sha256(manifest))) {
            throw new DatabaseValidationException("online index log header digest/CRC mismatch");
        }
        return new OnlineIndexLogHeader(OnlineIndexBuildId.of(buildId), tableId, indexId,
                sourceVersion, targetVersion, rowFormatVersion, manifest);
    }

    /**
     * 编码一条frame；CRC覆盖version到payload，total length独立用于scanner界定尾帧。
     *
     * @param record 已验证的frame领域对象
     * @return 含总长度前缀和尾CRC的独立字节
     * @throws DatabaseValidationException payload使frame越过v1安全上限时抛出
     */
    public byte[] encodeRecord(OnlineIndexLogRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("online index log record must not be null");
        }
        byte[] payload = record.payload();
        int frameLength;
        try {
            frameLength = Math.addExact(MIN_FRAME_BYTES, payload.length);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("online index log frame length overflow", error);
        }
        if (frameLength > MAX_FRAME_BYTES) {
            throw new DatabaseValidationException("online index log frame exceeds format limit: " + frameLength);
        }
        ByteBuffer encoded = ByteBuffer.allocate(frameLength).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(frameLength).putShort((short) FORMAT_VERSION)
                .putShort((short) record.type().stableCode())
                .putLong(record.generation()).putLong(record.sequence())
                .putLong(record.transactionId()).putInt(payload.length).put(payload);
        encoded.putInt(crc32c(encoded.array(), Integer.BYTES,
                encoded.position() - Integer.BYTES));
        return encoded.array();
    }

    /**
     * 解码一条由文件scanner界定的完整frame。
     *
     * @param source limit必须恰好等于声明frame length
     * @return 完成CRC与领域边界校验的frame
     * @throws DatabaseValidationException 截断、尾随、未知类型或CRC损坏时抛出
     */
    public OnlineIndexLogRecord decodeRecord(ByteBuffer source) {
        if (source == null) {
            throw new DatabaseValidationException("online index record source must not be null");
        }
        ByteBuffer input = source.slice().order(ByteOrder.BIG_ENDIAN);
        int actualLength = input.remaining();
        if (actualLength < MIN_FRAME_BYTES) {
            throw new DatabaseValidationException("online index log frame is truncated");
        }
        int declaredLength = input.getInt();
        int version = Short.toUnsignedInt(input.getShort());
        int typeCode = Short.toUnsignedInt(input.getShort());
        if (declaredLength != actualLength || declaredLength > MAX_FRAME_BYTES
                || version != FORMAT_VERSION) {
            throw new DatabaseValidationException("online index log frame length/version mismatch");
        }
        long generation = input.getLong();
        long sequence = input.getLong();
        long transactionId = input.getLong();
        int payloadLength = input.getInt();
        if (payloadLength < 0 || input.remaining() != payloadLength + Integer.BYTES) {
            throw new DatabaseValidationException("online index log frame payload length mismatch");
        }
        byte[] payload = new byte[payloadLength];
        input.get(payload);
        int expectedCrc = input.getInt();
        ByteBuffer original = source.slice();
        byte[] bytes = new byte[original.remaining()];
        original.get(bytes);
        int actualCrc = crc32c(bytes, Integer.BYTES,
                bytes.length - Integer.BYTES * 2);
        if (expectedCrc != actualCrc) {
            throw new DatabaseValidationException("online index log frame CRC mismatch");
        }
        return new OnlineIndexLogRecord(OnlineIndexLogRecordType.fromStableCode(typeCode),
                generation, sequence, transactionId, payload);
    }

    /** @return header前缀中声明的完整长度；输入position不改变。 */
    int declaredHeaderLength(ByteBuffer prefix) {
        if (prefix == null || prefix.remaining() < HEADER_PREFIX_BYTES) {
            throw new DatabaseValidationException("online index log header prefix is truncated");
        }
        ByteBuffer input = prefix.slice().order(ByteOrder.BIG_ENDIAN);
        if (input.getLong() != HEADER_MAGIC || input.getInt() != FORMAT_VERSION) {
            throw new DatabaseValidationException("online index log header prefix magic/version mismatch");
        }
        int length = input.getInt();
        if (length < HEADER_PREFIX_BYTES + Long.BYTES * 6 + Integer.BYTES
                + DIGEST_BYTES + Integer.BYTES || length > MAX_MANIFEST_BYTES + 4096) {
            throw new DatabaseValidationException("online index log declared header length is invalid: " + length);
        }
        return length;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException error) {
            throw new DatabaseValidationException("SHA-256 is unavailable for online index manifest", error);
        }
    }

    private static int crc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
