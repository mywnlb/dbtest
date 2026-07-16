package cn.zhangyis.db.storage.fil.catalog;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogCorruptionException;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogPersistenceException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/**
 * `mysql.ibd` v1 的 page-aligned append-only catalog 实现。两个 4 KiB header slot 持有 committedLength；data
 * 先 force，再切换 header generation 并 force，因此 crash 尾部永远不会被当作已提交字典。每个 frame 用 CRC32C，
 * 每个 batch 用 SHA-256，已发布边界内任一损坏都 fail-closed。
 *
 * <p>这是当前 teaching slice 的内部 catalog 物理后端：它保留稳定 {@link InternalCatalogStore} 边界，后续可在
 * 不改变 DD repository 的前提下替换成 catalog B+Tree；与 MySQL 的差异会同步记录在厚设计和 current map。
 */
public final class FileInternalCatalogStore implements InternalCatalogStore {

    static final int HEADER_SLOT_BYTES = 4096;
    public static final long DATA_START = HEADER_SLOT_BYTES * 2L;

    private static final long HEADER_MAGIC = 0x4D_49_4E_49_44_44_49_42L; // MINIDDIB
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_CRC_OFFSET = HEADER_SLOT_BYTES - Integer.BYTES;
    private static final byte DATA_FRAME = 1;
    private static final byte COMMIT_FRAME = 2;
    private static final int FRAME_PREFIX_BYTES = Integer.BYTES * 2;
    private static final int MAX_KEY_BYTES = 256;
    private static final int MAX_PAYLOAD_BYTES = 1024;
    private static final int SHA256_BYTES = 32;

    /** 串行 append/header 发布与 close；锁内只做本 catalog 文件 IO。 */
    private final ReentrantLock ioLock = new ReentrantLock();

    /** 本 store 独占的 positional IO channel。 */
    private final FileChannel channel;

    /** 诊断路径。 */
    private final Path path;

    /** 最近有效 header；只在 ioLock 下替换。 */
    private Header current;

    /** 启动时完整校验后重建、append 成功后追加的 committed 批次视图。 */
    private final List<CatalogBatch> batches;

    private FileInternalCatalogStore(Path path, FileChannel channel, Header current, List<CatalogBatch> batches) {
        this.path = path;
        this.channel = channel;
        this.current = current;
        this.batches = new ArrayList<>(batches);
    }

    /** 新建空 catalog 或打开既有文件；空文件会先 durable 初始 generation。 */
    public static FileInternalCatalogStore openOrCreate(Path path) {
        validatePath(path);
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            boolean exists = Files.exists(path);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            if (exists && channel.size() > 0) {
                return openValidated(path, channel);
            }
            channel.truncate(DATA_START);
            Header initial = new Header(1, DATA_START, 1);
            writeHeader(channel, initial);
            channel.force(true);
            return new FileInternalCatalogStore(path, channel, initial, List.of());
        } catch (IOException e) {
            throw new InternalCatalogPersistenceException("open/create internal catalog failed: " + path, e);
        }
    }

    /** 打开必须已经存在且包含至少一个有效 header 的 catalog。 */
    public static FileInternalCatalogStore openExisting(Path path) {
        validatePath(path);
        try {
            return openValidated(path, FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE));
        } catch (IOException e) {
            throw new InternalCatalogPersistenceException("open internal catalog failed: " + path, e);
        }
    }

    private static FileInternalCatalogStore openValidated(Path path, FileChannel channel) {
        try {
            List<Header> headers = new ArrayList<>(2);
            decodeHeader(channel, 0).ifPresent(headers::add);
            decodeHeader(channel, 1).ifPresent(headers::add);
            Header latest = headers.stream().max(Comparator.comparingLong(Header::generation))
                    .orElseThrow(() -> new InternalCatalogCorruptionException(
                            "both internal catalog headers are invalid: " + path));
            if (latest.committedLength < DATA_START || latest.committedLength > channel.size()) {
                throw new InternalCatalogCorruptionException("internal catalog committed length is invalid: "
                        + latest.committedLength + ", file=" + channel.size());
            }
            List<CatalogBatch> batches = scanCommitted(channel, latest);
            return new FileInternalCatalogStore(path, channel, latest, batches);
        } catch (RuntimeException | IOException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new InternalCatalogPersistenceException("validate internal catalog failed: " + path, failure);
        }
    }

    /**
     * 追加批次的数据流：覆盖旧 generation 之后的未发布 crash tail → 写 data frames/commit frame → force data →
     * 写另一 header slot/force metadata → 发布内存 header。任何异常都不会更新 current/batches。
     */
    @Override
    public long append(List<CatalogRecord> records) {
        validateRecords(records);
        ioLock.lock();
        try {
            long sequence = current.nextBatchSequence;
            long position = current.committedLength;
            try {
                channel.truncate(position);
                MessageDigest digest = sha256();
                int ordinal = 0;
                for (CatalogRecord record : records) {
                    byte[] body = dataBody(sequence, ordinal++, record);
                    digest.update(body);
                    position = writeFrame(channel, position, body);
                }
                byte[] commit = commitBody(sequence, records.size(), digest.digest());
                position = writeFrame(channel, position, commit);
                channel.force(false);
                Header next = new Header(Math.addExact(current.generation, 1), position,
                        Math.addExact(sequence, 1));
                writeHeader(channel, next);
                channel.force(true);
                current = next;
                batches.add(new CatalogBatch(sequence, List.copyOf(records)));
                return sequence;
            } catch (IOException | ArithmeticException e) {
                throw new InternalCatalogPersistenceException("append internal catalog batch failed: " + path, e);
            }
        } finally {
            ioLock.unlock();
        }
    }

    @Override
    public List<CatalogBatch> readCommittedBatches() {
        ioLock.lock();
        try {
            return List.copyOf(batches);
        } finally {
            ioLock.unlock();
        }
    }

    @Override
    public long committedLength() {
        ioLock.lock();
        try {
            return current.committedLength;
        } finally {
            ioLock.unlock();
        }
    }

    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new InternalCatalogPersistenceException("close internal catalog failed: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private static List<CatalogBatch> scanCommitted(FileChannel channel, Header header) throws IOException {
        List<CatalogBatch> result = new ArrayList<>();
        long position = DATA_START;
        long expectedSequence = 1;
        List<CatalogRecord> pending = new ArrayList<>();
        MessageDigest digest = sha256();
        while (position < header.committedLength) {
            Frame frame = readFrame(channel, position, header.committedLength);
            position = frame.endOffset;
            ByteBuffer body = ByteBuffer.wrap(frame.body).order(ByteOrder.BIG_ENDIAN);
            byte type = body.get();
            long sequence = body.getLong();
            int ordinal = body.getInt();
            int keyLength = body.getInt();
            int payloadLength = body.getInt();
            if (sequence != expectedSequence) {
                throw new InternalCatalogCorruptionException("catalog batch sequence gap: expected="
                        + expectedSequence + ", actual=" + sequence);
            }
            if (type == DATA_FRAME) {
                if (ordinal != pending.size() || keyLength <= 0 || keyLength > MAX_KEY_BYTES
                        || payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                        || body.remaining() != keyLength + payloadLength) {
                    throw new InternalCatalogCorruptionException("invalid internal catalog data frame");
                }
                byte[] key = new byte[keyLength];
                byte[] payload = new byte[payloadLength];
                body.get(key);
                body.get(payload);
                digest.update(frame.body);
                pending.add(new CatalogRecord(key, payload));
            } else if (type == COMMIT_FRAME) {
                if (ordinal != pending.size() || keyLength != 0 || payloadLength != SHA256_BYTES
                        || body.remaining() != SHA256_BYTES) {
                    throw new InternalCatalogCorruptionException("invalid internal catalog commit frame");
                }
                byte[] expectedHash = new byte[SHA256_BYTES];
                body.get(expectedHash);
                if (!Arrays.equals(expectedHash, digest.digest()) || pending.isEmpty()) {
                    throw new InternalCatalogCorruptionException("internal catalog batch hash mismatch");
                }
                result.add(new CatalogBatch(sequence, List.copyOf(pending)));
                pending.clear();
                digest = sha256();
                expectedSequence++;
            } else {
                throw new InternalCatalogCorruptionException("unknown internal catalog frame type: " + type);
            }
        }
        if (position != header.committedLength || !pending.isEmpty()
                || expectedSequence != header.nextBatchSequence) {
            throw new InternalCatalogCorruptionException("internal catalog header/batch boundary mismatch");
        }
        return result;
    }

    private static byte[] dataBody(long sequence, int ordinal, CatalogRecord record) {
        byte[] key = record.key();
        byte[] payload = record.payload();
        ByteBuffer body = ByteBuffer.allocate(1 + Long.BYTES + Integer.BYTES * 3 + key.length + payload.length)
                .order(ByteOrder.BIG_ENDIAN);
        body.put(DATA_FRAME).putLong(sequence).putInt(ordinal).putInt(key.length).putInt(payload.length)
                .put(key).put(payload);
        return body.array();
    }

    private static byte[] commitBody(long sequence, int count, byte[] hash) {
        ByteBuffer body = ByteBuffer.allocate(1 + Long.BYTES + Integer.BYTES * 3 + hash.length)
                .order(ByteOrder.BIG_ENDIAN);
        body.put(COMMIT_FRAME).putLong(sequence).putInt(count).putInt(0).putInt(hash.length).put(hash);
        return body.array();
    }

    private static long writeFrame(FileChannel channel, long position, byte[] body) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(FRAME_PREFIX_BYTES + body.length).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(body.length).putInt(crc(body)).put(body).flip();
        writeFully(channel, frame, position);
        return position + frame.capacity();
    }

    private static Frame readFrame(FileChannel channel, long position, long committedLength) throws IOException {
        if (position + FRAME_PREFIX_BYTES > committedLength) {
            throw new InternalCatalogCorruptionException("truncated internal catalog frame prefix");
        }
        ByteBuffer prefix = ByteBuffer.allocate(FRAME_PREFIX_BYTES).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, prefix, position);
        prefix.flip();
        int length = prefix.getInt();
        int expectedCrc = prefix.getInt();
        int maxFrameBody = 1 + Long.BYTES + Integer.BYTES * 3 + MAX_KEY_BYTES + MAX_PAYLOAD_BYTES;
        if (length < 1 + Long.BYTES + Integer.BYTES * 3 || length > maxFrameBody
                || position + FRAME_PREFIX_BYTES + length > committedLength) {
            throw new InternalCatalogCorruptionException("invalid internal catalog frame length: " + length);
        }
        ByteBuffer body = ByteBuffer.allocate(length);
        readFully(channel, body, position + FRAME_PREFIX_BYTES);
        if (crc(body.array()) != expectedCrc) {
            throw new InternalCatalogCorruptionException("internal catalog frame CRC mismatch");
        }
        return new Frame(body.array(), position + FRAME_PREFIX_BYTES + length);
    }

    private static void writeHeader(FileChannel channel, Header header) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(HEADER_MAGIC).putInt(FORMAT_VERSION).putLong(header.generation)
                .putLong(header.committedLength).putLong(header.nextBatchSequence);
        buffer.position(HEADER_CRC_OFFSET);
        buffer.putInt(crc(buffer.array(), HEADER_CRC_OFFSET));
        buffer.flip();
        writeFully(channel, buffer, Math.floorMod(header.generation, 2) * (long) HEADER_SLOT_BYTES);
    }

    private static Optional<Header> decodeHeader(FileChannel channel, int slot) {
        try {
            if (channel.size() < (slot + 1L) * HEADER_SLOT_BYTES) {
                return Optional.empty();
            }
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, buffer, slot * (long) HEADER_SLOT_BYTES);
            byte[] bytes = buffer.array();
            if (ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(HEADER_CRC_OFFSET)
                    != crc(bytes, HEADER_CRC_OFFSET)) {
                return Optional.empty();
            }
            buffer.flip();
            if (buffer.getLong() != HEADER_MAGIC || buffer.getInt() != FORMAT_VERSION) {
                return Optional.empty();
            }
            Header header = new Header(buffer.getLong(), buffer.getLong(), buffer.getLong());
            if (header.generation <= 0 || header.committedLength < DATA_START || header.nextBatchSequence <= 0) {
                return Optional.empty();
            }
            return Optional.of(header);
        } catch (IOException | RuntimeException ignored) {
            return Optional.empty();
        }
    }

    private static void validateRecords(List<CatalogRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new DatabaseValidationException("internal catalog batch must not be null or empty");
        }
        for (CatalogRecord record : records) {
            if (record == null || record.key().length > MAX_KEY_BYTES || record.payload().length > MAX_PAYLOAD_BYTES) {
                throw new DatabaseValidationException("internal catalog record exceeds key/payload bounds");
            }
        }
    }

    private static void validatePath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("internal catalog path must not be null");
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new InternalCatalogCorruptionException("JVM does not provide SHA-256", e);
        }
    }

    private static int crc(byte[] bytes) {
        return crc(bytes, bytes.length);
    }

    private static int crc(byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written <= 0) {
                throw new IOException("internal catalog write made no progress");
            }
            position += written;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in internal catalog");
            }
            if (read == 0) {
                throw new IOException("internal catalog read made no progress");
            }
            position += read;
        }
    }

    private record Header(long generation, long committedLength, long nextBatchSequence) {
    }

    private record Frame(byte[] body, long endOffset) {
    }
}
