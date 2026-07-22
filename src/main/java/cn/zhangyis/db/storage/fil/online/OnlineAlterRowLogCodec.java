package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.zip.CRC32C;

/** `OALTLOG1` header/frame codec；只解释framing，不依赖DD manifest类型。 */
public final class OnlineAlterRowLogCodec {

    private static final long HEADER_MAGIC = 0x4f414c544c4f4731L; // OALTLOG1
    private static final int FORMAT_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_MANIFEST_BYTES = 1024 * 1024;
    static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;
    static final int HEADER_PREFIX_BYTES = Long.BYTES + Integer.BYTES * 2;
    private static final int HEADER_FIXED_BYTES = Long.BYTES + Integer.BYTES * 2
            + Long.BYTES * 7 + Integer.BYTES * 3 + DIGEST_BYTES + Integer.BYTES;
    static final int MIN_FRAME_BYTES = Integer.BYTES + Short.BYTES * 2
            + Long.BYTES * 3 + Integer.BYTES * 2;

    /**
     * 从固定前缀读取声明的完整header长度，供文件恢复在分配manifest缓冲区前执行上界检查。
     *
     * @param source offset 0读取的精确16字节前缀；position不会被调用方继续依赖
     * @return 包含CRC与manifest的完整正字节数
     * @throws DatabaseValidationException magic、版本或长度越界时抛出，调用方不得继续扫描frame
     */
    public int declaredHeaderLength(ByteBuffer source) {
        ByteBuffer input = exactBuffer(source, HEADER_PREFIX_BYTES, HEADER_PREFIX_BYTES,
                "header prefix");
        if (input.getLong() != HEADER_MAGIC || input.getInt() != FORMAT_VERSION) {
            throw new DatabaseValidationException("online ALTER header prefix magic/version is invalid");
        }
        int length = input.getInt();
        if (length < HEADER_FIXED_BYTES || length > HEADER_FIXED_BYTES + MAX_MANIFEST_BYTES) {
            throw new DatabaseValidationException("online ALTER declared header length is invalid");
        }
        return length;
    }

    public byte[] encodeHeader(OnlineAlterLogHeader header) {
        if (header == null) {
            throw new DatabaseValidationException("online ALTER log header must not be null");
        }
        byte[] manifest = header.manifest();
        if (manifest.length > MAX_MANIFEST_BYTES) {
            throw new DatabaseValidationException("online ALTER manifest exceeds log header limit");
        }
        int total;
        try {
            total = Math.addExact(HEADER_FIXED_BYTES, manifest.length);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("online ALTER header length overflows", overflow);
        }
        ByteBuffer output = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        output.putLong(HEADER_MAGIC).putInt(FORMAT_VERSION).putInt(total)
                .putLong(header.captureId().value()).putLong(header.tableId())
                .putLong(header.sourceDictionaryVersion()).putLong(header.targetDictionaryVersion())
                .putLong(header.sourceRowFormatVersion()).putLong(header.targetRowFormatVersion())
                .putInt(header.executionProtocolCode()).putInt(header.shadowSpaceId())
                .putLong(header.freezeReadViewGeneration())
                .putInt(manifest.length).put(sha256(manifest)).put(manifest);
        output.putInt(crc32c(Arrays.copyOf(output.array(), total - Integer.BYTES)));
        return output.array();
    }

    public OnlineAlterLogHeader decodeHeader(ByteBuffer source) {
        ByteBuffer input = exactBuffer(source, HEADER_FIXED_BYTES, Integer.MAX_VALUE, "header");
        int total = input.remaining();
        byte[] bytes = new byte[total];
        input.get(bytes);
        ByteBuffer fields = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (fields.getLong() != HEADER_MAGIC || fields.getInt() != FORMAT_VERSION
                || fields.getInt() != total) {
            throw new DatabaseValidationException("online ALTER header magic/version/length is invalid");
        }
        long captureId = fields.getLong();
        long tableId = fields.getLong();
        long sourceVersion = fields.getLong();
        long targetVersion = fields.getLong();
        long sourceRowFormat = fields.getLong();
        long targetRowFormat = fields.getLong();
        int protocol = fields.getInt();
        int shadowSpace = fields.getInt();
        long freezeGeneration = fields.getLong();
        int manifestLength = fields.getInt();
        if (manifestLength <= 0 || manifestLength > MAX_MANIFEST_BYTES
                || fields.remaining() != DIGEST_BYTES + manifestLength + Integer.BYTES) {
            throw new DatabaseValidationException("online ALTER header manifest length is invalid");
        }
        byte[] digest = new byte[DIGEST_BYTES];
        fields.get(digest);
        byte[] manifest = new byte[manifestLength];
        fields.get(manifest);
        int expectedCrc = fields.getInt();
        if (expectedCrc != crc32c(Arrays.copyOf(bytes, bytes.length - Integer.BYTES))
                || !MessageDigest.isEqual(digest, sha256(manifest))) {
            throw new DatabaseValidationException("online ALTER header digest/CRC mismatch");
        }
        return new OnlineAlterLogHeader(OnlineDdlCaptureId.of(captureId), tableId,
                sourceVersion, targetVersion, sourceRowFormat, targetRowFormat,
                protocol, shadowSpace, freezeGeneration, manifest);
    }

    public byte[] encodeRecord(OnlineAlterLogRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("online ALTER log record must not be null");
        }
        byte[] payload = record.payload();
        int total;
        try {
            total = Math.addExact(MIN_FRAME_BYTES, payload.length);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("online ALTER frame length overflows", overflow);
        }
        if (total > MAX_FRAME_BYTES) {
            throw new DatabaseValidationException("online ALTER frame exceeds format limit");
        }
        ByteBuffer output = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        output.putInt(total).putShort((short) FORMAT_VERSION)
                .putShort((short) record.type().stableCode())
                .putLong(record.generation()).putLong(record.sequence())
                .putLong(record.transactionId()).putInt(payload.length).put(payload);
        output.putInt(crc32c(Arrays.copyOfRange(output.array(), Integer.BYTES,
                total - Integer.BYTES)));
        return output.array();
    }

    public OnlineAlterLogRecord decodeRecord(ByteBuffer source) {
        ByteBuffer input = exactBuffer(source, MIN_FRAME_BYTES, MAX_FRAME_BYTES, "frame");
        int total = input.remaining();
        byte[] bytes = new byte[total];
        input.get(bytes);
        ByteBuffer fields = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int declaredLength = fields.getInt();
        int version = Short.toUnsignedInt(fields.getShort());
        OnlineAlterLogRecordType type = OnlineAlterLogRecordType.fromStableCode(
                Short.toUnsignedInt(fields.getShort()));
        long generation = fields.getLong();
        long sequence = fields.getLong();
        long transactionId = fields.getLong();
        int payloadLength = fields.getInt();
        if (declaredLength != total || version != FORMAT_VERSION || payloadLength < 0
                || fields.remaining() != payloadLength + Integer.BYTES) {
            throw new DatabaseValidationException("online ALTER frame shape is invalid");
        }
        byte[] payload = new byte[payloadLength];
        fields.get(payload);
        int expectedCrc = fields.getInt();
        int actualCrc = crc32c(Arrays.copyOfRange(bytes, Integer.BYTES,
                bytes.length - Integer.BYTES));
        if (expectedCrc != actualCrc) {
            throw new DatabaseValidationException("online ALTER frame CRC32C mismatch");
        }
        return new OnlineAlterLogRecord(type, generation, sequence, transactionId, payload);
    }

    private static ByteBuffer exactBuffer(ByteBuffer source, int min, int max, String field) {
        if (source == null) {
            throw new DatabaseValidationException("online ALTER " + field + " must not be null");
        }
        ByteBuffer input = source.slice().order(ByteOrder.BIG_ENDIAN);
        if (input.remaining() < min || input.remaining() > max) {
            throw new DatabaseValidationException("online ALTER " + field + " length is invalid");
        }
        return input;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException error) {
            throw new DatabaseFatalException("SHA-256 unavailable for online ALTER log", error);
        }
    }

    private static int crc32c(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
