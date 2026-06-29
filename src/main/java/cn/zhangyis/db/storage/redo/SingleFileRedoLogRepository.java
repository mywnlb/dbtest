package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redo 物理文件仓储（R1 简化版，单文件 append-only）。批次帧格式由 {@link RedoBatchFrameCodec} 统一定义。
 *
 * <p>简化点：只实现单 redo 文件、同步追加和扫描；不做循环文件、capacity/checkpoint 回收和 log block 对齐——这些由
 * {@link RotatingRedoLogRepository}（0.18 文件环）承担。完整 frame 的结构错误视为致命损坏，不完整尾部视为 crash
 * 截断点，由扫描停止。
 */
public final class SingleFileRedoLogRepository implements RedoLogFileRepository {

    /** redo 文件路径，用于异常诊断。 */
    private final Path path;
    /** redo 文件 channel；追加、force、扫描都通过该对象。 */
    private final FileChannel channel;
    /** 保护 channel position/force/read 扫描，避免追加和恢复读取互相干扰。 */
    private final ReentrantLock ioLock = new ReentrantLock();

    private SingleFileRedoLogRepository(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
    }

    /**
     * 打开或创建单个 redo 文件。父目录不存在时自动创建；文件内容保留，追加写从当前文件末尾开始。
     *
     * @param path redo 文件路径。
     * @return 已打开仓储。
     */
    public static SingleFileRedoLogRepository open(Path path) {
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
            return new SingleFileRedoLogRepository(path, channel);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open redo log file: " + path, e);
        }
    }

    @Override
    public void append(RedoLogBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
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

    @Override
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

    @Override
    public List<RedoLogBatch> readBatches() {
        ioLock.lock();
        try {
            List<RedoLogBatch> out = new ArrayList<>();
            channel.position(0);
            while (true) {
                ByteBuffer header = ByteBuffer.allocate(RedoBatchFrameCodec.FRAME_HEADER_BYTES);
                if (!readFullyOrTail(header)) {
                    break;
                }
                header.flip();
                int magic = header.getInt();
                if (magic != RedoBatchFrameCodec.MAGIC) {
                    throw new RedoLogCorruptedException("redo frame magic mismatch in " + path + ": " + magic);
                }
                int payloadLength = header.getInt();
                int expectedCrc = header.getInt();
                if (payloadLength <= 0 || payloadLength > RedoBatchFrameCodec.MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("redo frame payload length invalid in "
                            + path + ": " + payloadLength);
                }
                ByteBuffer payload = ByteBuffer.allocate(payloadLength);
                if (!readFullyOrTail(payload)) {
                    break;
                }
                byte[] bytes = payload.array();
                if (RedoBatchFrameCodec.crc32(bytes) != expectedCrc) {
                    throw new RedoLogCorruptedException("redo frame checksum mismatch in " + path);
                }
                out.add(RedoBatchFrameCodec.decodePayload(bytes));
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
}
