package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.page.PageType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Redo 物理文件仓储（R1 简化版）。文件按批次追加，每个批次 = magic + payload length + payload checksum + payload。
 *
 * <p>简化点：当前只实现单 redo 文件、同步追加和扫描；不做循环文件、capacity/checkpoint 回收和 log block 对齐。完整尾部
 * 之前的结构错误视为致命损坏，不完整尾部视为 crash 截断点，由 reader 停止扫描。
 */
public final class RedoLogFileRepository implements AutoCloseable {

    /** 文件批次 magic：ASCII "RLG1"。 */
    private static final int MAGIC = 0x524C4731;
    /** 批次外层头：magic(4) + payloadLength(4) + crc32(4)。 */
    private static final int FRAME_HEADER_BYTES = 12;
    /** payload 内层头：startLsn(8) + endLsn(8) + recordCount(4)。 */
    private static final int PAYLOAD_HEADER_BYTES = 20;
    /** 最小 record：tag(1) + pageId(space 4 + pageNo 8) + type(4)。 */
    private static final int MIN_RECORD_BYTES = 17;
    /** 防止损坏 length 触发大内存分配；教学实现单批 32MiB 足够。 */
    private static final int MAX_PAYLOAD_BYTES = 32 * 1024 * 1024;

    /** redo 文件路径，用于异常诊断。 */
    private final Path path;
    /** redo 文件 channel；追加、force、扫描都通过该对象。 */
    private final FileChannel channel;
    /** 保护 channel position/force/read 扫描，避免追加和恢复读取互相干扰。 */
    private final ReentrantLock ioLock = new ReentrantLock();

    private RedoLogFileRepository(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
    }

    /**
     * 打开或创建单个 redo 文件。父目录不存在时自动创建；文件内容保留，追加写从当前文件末尾开始。
     *
     * @param path redo 文件路径。
     * @return 已打开仓储。
     */
    public static RedoLogFileRepository open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("redo log path must not be null");
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new RedoLogFileRepository(path, channel);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open redo log file: " + path, e);
        }
    }

    /**
     * 追加一个完整 redo 批次。调用方负责按 LSN 顺序调用；本仓储只保证单批 bytes 原样追加。
     *
     * @param batch 待写入批次。
     */
    public void append(RedoLogBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        ByteBuffer frame = encodeFrame(batch);
        ioLock.lock();
        try {
            channel.position(channel.size());
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
        } catch (IOException e) {
            throw new RedoLogIoException("failed to append redo batch to file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 对 redo 文件执行 fsync/force，调用成功后 writer 已写入的 LSN 才能发布为 flushedToDiskLsn。 */
    public void force() {
        ioLock.lock();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to force redo log file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 顺序扫描完整 redo 批次。遇到不完整 frame header 或 payload 时停止，作为 crash 后的 torn tail 处理。
     *
     * @return 完整可校验批次。
     */
    public List<RedoLogBatch> readBatches() {
        ioLock.lock();
        try {
            List<RedoLogBatch> out = new ArrayList<>();
            channel.position(0);
            while (true) {
                ByteBuffer header = ByteBuffer.allocate(FRAME_HEADER_BYTES);
                if (!readFullyOrTail(header)) {
                    break;
                }
                header.flip();
                int magic = header.getInt();
                if (magic != MAGIC) {
                    throw new RedoLogCorruptedException("redo frame magic mismatch in " + path + ": " + magic);
                }
                int payloadLength = header.getInt();
                int expectedCrc = header.getInt();
                if (payloadLength <= 0 || payloadLength > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("redo frame payload length invalid in "
                            + path + ": " + payloadLength);
                }
                ByteBuffer payload = ByteBuffer.allocate(payloadLength);
                if (!readFullyOrTail(payload)) {
                    break;
                }
                byte[] bytes = payload.array();
                if (crc32(bytes) != expectedCrc) {
                    throw new RedoLogCorruptedException("redo frame checksum mismatch in " + path);
                }
                out.add(decodePayload(bytes));
            }
            return out;
        } catch (IOException e) {
            throw new RedoLogIoException("failed to read redo log file: " + path, e);
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
            throw new RedoLogIoException("failed to close redo log file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private boolean readFullyOrTail(ByteBuffer dst) throws IOException {
        while (dst.hasRemaining()) {
            int n = channel.read(dst);
            if (n < 0) {
                return false;
            }
            if (n == 0) {
                return false;
            }
        }
        return true;
    }

    private static ByteBuffer encodeFrame(RedoLogBatch batch) {
        byte[] payload = encodePayload(batch);
        ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_BYTES + payload.length);
        frame.putInt(MAGIC);
        frame.putInt(payload.length);
        frame.putInt(crc32(payload));
        frame.put(payload);
        frame.flip();
        return frame;
    }

    private static byte[] encodePayload(RedoLogBatch batch) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(batch.range().start().value());
            out.writeLong(batch.range().end().value());
            out.writeInt(batch.records().size());
            for (RedoRecord r : batch.records()) {
                writeRecord(out, r);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to encode redo payload", e);
        }
    }

    private static void writeRecord(DataOutputStream out, RedoRecord r) throws IOException {
        PageId pageId = pageIdOf(r);
        if (r instanceof PageInitRecord pir) {
            out.writeByte(RedoRecordType.PAGE_INIT.tag());
            writePageId(out, pageId);
            out.writeInt(pir.pageType().code());
        } else if (r instanceof PageBytesRecord pbr) {
            out.writeByte(RedoRecordType.PAGE_BYTES.tag());
            writePageId(out, pageId);
            out.writeInt(pbr.offset());
            byte[] bytes = pbr.bytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else {
            throw new RedoLogCorruptedException("unsupported redo record type: " + r.getClass().getName());
        }
    }

    private static RedoLogBatch decodePayload(byte[] bytes) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            LogRange range = new LogRange(Lsn.of(in.readLong()), Lsn.of(in.readLong()));
            int count = in.readInt();
            int remaining = bytes.length - PAYLOAD_HEADER_BYTES;
            if (count < 0 || count > remaining / MIN_RECORD_BYTES) {
                throw new RedoLogCorruptedException("redo record count invalid: " + count);
            }
            List<RedoRecord> records = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                records.add(readRecord(in));
            }
            if (in.available() != 0) {
                throw new RedoLogCorruptedException("redo payload has trailing bytes: " + in.available());
            }
            return new RedoLogBatch(range, records);
        } catch (IOException e) {
            throw new RedoLogCorruptedException("failed to decode redo payload", e);
        } catch (DatabaseValidationException e) {
            throw new RedoLogCorruptedException("redo payload contains invalid domain value", e);
        }
    }

    private static RedoRecord readRecord(DataInputStream in) throws IOException {
        RedoRecordType type = RedoRecordType.fromTag(in.readByte());
        PageId pageId = readPageId(in);
        return switch (type) {
            case PAGE_INIT -> new PageInitRecord(pageId, PageType.fromCode(in.readInt()));
            case PAGE_BYTES -> {
                int offset = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("redo bytes length invalid: " + len);
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    throw new RedoLogCorruptedException("redo bytes payload truncated");
                }
                yield new PageBytesRecord(pageId, offset, payload);
            }
        };
    }

    private static void writePageId(DataOutputStream out, PageId pageId) throws IOException {
        out.writeInt(pageId.spaceId().value());
        out.writeLong(pageId.pageNo().value());
    }

    private static PageId readPageId(DataInputStream in) throws IOException {
        return PageId.of(SpaceId.of(in.readInt()), PageNo.of(in.readLong()));
    }

    private static PageId pageIdOf(RedoRecord r) {
        if (r instanceof PageInitRecord pir) {
            return pir.pageId();
        }
        if (r instanceof PageBytesRecord pbr) {
            return pbr.pageId();
        }
        throw new RedoLogCorruptedException("unsupported redo record type: " + r.getClass().getName());
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
