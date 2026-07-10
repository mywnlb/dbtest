package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * 事务恢复高水位 sidecar 仓储。
 *
 * <p>文件固定包含两个 CRC slot，每个 slot 位于独立的 4 KiB 文件页，写入时交替覆盖并强制落盘。
 * 物理隔离避免一次 torn sector/page write 同时破坏两份副本。恢复只选择完整有效且 checkpoint 最大的副本；
 * checkpoint 相同时以单调 generation 选择较新 counter 快照。该文件先于 redo checkpoint label 持久化，
 * 因而 sidecar 比 redo label 更新是合法崩溃窗口，落后则由恢复编排层拒绝。
 */
public final class TransactionRecoveryCheckpointStore
        implements TransactionRecoveryCheckpointSource, AutoCloseable {

    /** slot magic：ASCII "TRC1"。 */
    private static final int MAGIC = 0x54524331;
    /** sidecar 二进制格式版本。 */
    private static final int FORMAT_VERSION = 1;
    /** magic/version/checkpoint/nextId/nextNo/generation/crc32 的固定长度。 */
    static final int SLOT_BYTES = 44;
    /** 两槽起始偏移之间的跨度；按 4 KiB 隔离，供整页损坏回退测试固定磁盘布局。 */
    static final int SLOT_STRIDE_BYTES = 4 * 1024;
    /** sidecar 固定双槽，预分配两个独立文件页。 */
    private static final int SLOT_COUNT = 2;
    /** 固定文件长度；尾部 padding 不参与 checksum 或版本语义。 */
    private static final long FILE_BYTES = (long) SLOT_COUNT * SLOT_STRIDE_BYTES;
    /** checksum 覆盖的 slot 前缀，不包含末尾 crc32。 */
    private static final int CHECKSUM_BYTES = SLOT_BYTES - Integer.BYTES;
    /** sidecar 路径，只用于生命周期和异常诊断。 */
    private final Path path;
    /** positional IO channel；共享 position 不参与正确性。 */
    private final FileChannel channel;
    /** 是否允许覆盖 slot；READ_ONLY_VALIDATE 打开的 store 固定为 false。 */
    private final boolean writable;
    /** 保护 slot 选择、写入、force 与关闭，等待仅限本地文件 IO。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 下一次覆盖的 slot，仅在 {@link #ioLock} 内读写。 */
    private int nextSlot;
    /** 下一次持久化 generation，仅在 {@link #ioLock} 内递增。 */
    private long nextGeneration;

    private TransactionRecoveryCheckpointStore(Path path, FileChannel channel, boolean writable) {
        this.path = path;
        this.channel = channel;
        this.writable = writable;
        Optional<Slot> latest = latestSlot();
        this.nextSlot = latest.map(slot -> 1 - slot.slotIndex()).orElse(0);
        this.nextGeneration = latest.map(slot -> slot.generation() + 1).orElse(1L);
    }

    /** 打开或创建事务恢复 sidecar；不会把空文件伪装成权威初始基线。 */
    public static TransactionRecoveryCheckpointStore open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("transaction recovery checkpoint path must not be null");
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            ensureFixedFileSize(channel);
            return new TransactionRecoveryCheckpointStore(path, channel, true);
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to open transaction recovery checkpoint sidecar: " + path, e);
        }
    }

    /**
     * 只读打开已存在 sidecar。该入口不创建目录/文件；路径缺失或并发删除会作为恢复输入 IO 错误 fail closed。
     */
    public static TransactionRecoveryCheckpointStore openReadOnly(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("read-only transaction recovery checkpoint path must not be null");
        }
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return new TransactionRecoveryCheckpointStore(path, channel, false);
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to open read-only transaction recovery checkpoint sidecar: " + path, e);
        }
    }

    /**
     * 读取最新有效基线。空文件或两个 slot 都损坏时返回 empty，交给恢复层结合 redo checkpoint 判定是否兼容。
     */
    @Override
    public Optional<TransactionRecoveryCheckpoint> readLatest() {
        ioLock.lock();
        try {
            return latestSlot().map(Slot::checkpoint);
        } finally {
            ioLock.unlock();
        }
    }

    /** 写入并 force 一个高水位快照；force 成功前不会切换下一槽或推进 generation。 */
    public void write(TransactionRecoveryCheckpoint checkpoint) {
        if (checkpoint == null) {
            throw new DatabaseValidationException("transaction recovery checkpoint must not be null");
        }
        if (!writable) {
            throw new TransactionRecoveryException(
                    "read-only transaction recovery checkpoint store cannot write: " + path);
        }
        ioLock.lock();
        try {
            ByteBuffer encoded = encode(checkpoint, nextGeneration);
            long offset = slotOffset(nextSlot);
            while (encoded.hasRemaining()) {
                channel.write(encoded, offset + encoded.position());
            }
            channel.force(true);
            nextSlot = 1 - nextSlot;
            nextGeneration++;
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to persist transaction recovery checkpoint to " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 关闭 sidecar channel；失败时保留底层 cause。 */
    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to close transaction recovery checkpoint sidecar: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private Optional<Slot> latestSlot() {
        Optional<Slot> first = readSlot(0);
        Optional<Slot> second = readSlot(1);
        if (first.isEmpty()) {
            return second;
        }
        if (second.isEmpty()) {
            return first;
        }
        return Optional.of(newer(first.get(), second.get()));
    }

    private Optional<Slot> readSlot(int slotIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        long offset = slotOffset(slotIndex);
        try {
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, offset + buffer.position());
                if (read <= 0) {
                    return Optional.empty();
                }
            }
            byte[] bytes = buffer.array();
            ByteBuffer view = ByteBuffer.wrap(bytes);
            if (view.getInt() != MAGIC || view.getInt() != FORMAT_VERSION) {
                return Optional.empty();
            }
            if (view.getInt(CHECKSUM_BYTES) != checksum(bytes)) {
                return Optional.empty();
            }
            TransactionRecoveryCheckpoint checkpoint = new TransactionRecoveryCheckpoint(
                    Lsn.of(view.getLong(8)), TransactionId.of(view.getLong(16)),
                    TransactionNo.of(view.getLong(24)));
            return Optional.of(new Slot(slotIndex, checkpoint, view.getLong(32)));
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to read transaction recovery checkpoint slot from " + path, e);
        } catch (DatabaseValidationException e) {
            return Optional.empty();
        }
    }

    private static Slot newer(Slot first, Slot second) {
        int checkpointOrder = Long.compare(
                first.checkpoint().checkpointLsn().value(), second.checkpoint().checkpointLsn().value());
        if (checkpointOrder != 0) {
            return checkpointOrder > 0 ? first : second;
        }
        return first.generation() >= second.generation() ? first : second;
    }

    private static ByteBuffer encode(TransactionRecoveryCheckpoint checkpoint, long generation) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putLong(checkpoint.checkpointLsn().value());
        buffer.putLong(checkpoint.nextTransactionId().value());
        buffer.putLong(checkpoint.nextTransactionNo().value());
        buffer.putLong(generation);
        buffer.putInt(checksum(buffer.array()));
        buffer.flip();
        return buffer;
    }

    private static int checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, CHECKSUM_BYTES);
        return (int) crc.getValue();
    }

    /**
     * 首次创建时预分配两个 4 KiB 文件页。这里只写零 padding，不生成合法 slot；因此空文件语义仍由 magic/CRC
     * 区分。扩展成功后立即 force，避免第一次 checkpoint 之前遗留不稳定的文件长度。
     */
    private static void ensureFixedFileSize(FileChannel channel) throws IOException {
        if (channel.size() >= FILE_BYTES) {
            return;
        }
        channel.write(ByteBuffer.wrap(new byte[]{0}), FILE_BYTES - 1);
        channel.force(true);
    }

    /** 返回指定 slot 的 4 KiB 对齐起始偏移。 */
    private static long slotOffset(int slotIndex) {
        return (long) slotIndex * SLOT_STRIDE_BYTES;
    }

    private record Slot(int slotIndex, TransactionRecoveryCheckpoint checkpoint, long generation) {
    }
}
