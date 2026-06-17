package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

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
 * redo control 文件仓储，负责持久化 fuzzy checkpoint label。
 *
 * <p>R2 使用两个固定 slot，写入时交替覆盖并 force control 文件。恢复读取时分别校验 magic、版本和 checksum，
 * 选择 checkpoint LSN 最大的有效 slot；这样单个 slot torn write 或损坏不会让 NORMAL 恢复丢失另一个可用 label。
 */
public final class RedoCheckpointStore implements AutoCloseable {

    /** slot magic：ASCII "RCP2"。 */
    private static final int MAGIC = 0x52435032;
    /** control slot 格式版本。 */
    private static final int FORMAT_VERSION = 1;
    /** slot payload：magic(4)+version(4)+checkpoint(8)+current(8)+createdAt(8)+crc32(4)。 */
    static final int SLOT_BYTES = 36;
    /** 参与 checksum 的前缀长度，不包含最后的 crc32 字段。 */
    private static final int CHECKSUM_BYTES = SLOT_BYTES - Integer.BYTES;
    /** control 文件目前固定两个 checkpoint label 副本。 */
    private static final int SLOT_COUNT = 2;

    /** control 文件路径，用于异常诊断。 */
    private final Path path;
    /** control 文件 channel，positional IO 避免依赖共享 position。 */
    private final FileChannel channel;
    /** 保护同一 control 文件上的 slot 选择、写入和 force。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 下一次写入的 slot 编号；根据打开时最新有效 slot 推导。 */
    private int nextSlot;

    private RedoCheckpointStore(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
        this.nextSlot = nextSlotAfterLatest();
    }

    /**
     * 打开或创建 redo control 文件。该文件独立于 R1 redo data file，避免改变已有 append-only redo frame 格式。
     *
     * @param path control 文件路径。
     * @return 已打开的 checkpoint store。
     */
    public static RedoCheckpointStore open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("redo checkpoint control path must not be null");
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new RedoCheckpointStore(path, channel);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open redo checkpoint control file: " + path, e);
        }
    }

    /**
     * 读取最新有效 checkpoint label。若 control 文件为空、两个 slot 都无效或只存在 torn slot，则返回初始 label。
     */
    public RedoCheckpointLabel readLatest() {
        ioLock.lock();
        try {
            Optional<Slot> first = readSlot(0);
            Optional<Slot> second = readSlot(1);
            if (first.isEmpty() && second.isEmpty()) {
                return RedoCheckpointLabel.initial();
            }
            if (first.isEmpty()) {
                return second.get().label();
            }
            if (second.isEmpty()) {
                return first.get().label();
            }
            return newer(first.get(), second.get()).label();
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 写入一个 checkpoint label 并 force control 文件。写入成功后才切换下一个 slot。
     *
     * @param label checkpoint label。
     */
    public void write(RedoCheckpointLabel label) {
        if (label == null) {
            throw new DatabaseValidationException("redo checkpoint label must not be null");
        }
        ByteBuffer encoded = encode(label);
        ioLock.lock();
        try {
            long offset = (long) nextSlot * SLOT_BYTES;
            while (encoded.hasRemaining()) {
                channel.write(encoded, offset + encoded.position());
            }
            channel.force(true);
            nextSlot = 1 - nextSlot;
        } catch (IOException e) {
            throw new RedoLogIoException("failed to write redo checkpoint label to " + path, e);
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
            throw new RedoLogIoException("failed to close redo checkpoint control file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private int nextSlotAfterLatest() {
        Optional<Slot> first = readSlot(0);
        Optional<Slot> second = readSlot(1);
        if (first.isEmpty() && second.isEmpty()) {
            return 0;
        }
        if (first.isEmpty()) {
            return 0;
        }
        if (second.isEmpty()) {
            return 1;
        }
        return 1 - newer(first.get(), second.get()).slotIndex();
    }

    private Optional<Slot> readSlot(int slot) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        long offset = (long) slot * SLOT_BYTES;
        try {
            while (buffer.hasRemaining()) {
                int n = channel.read(buffer, offset + buffer.position());
                if (n < 0) {
                    return Optional.empty();
                }
                if (n == 0) {
                    return Optional.empty();
                }
            }
            byte[] bytes = buffer.array();
            ByteBuffer view = ByteBuffer.wrap(bytes);
            if (view.getInt() != MAGIC) {
                return Optional.empty();
            }
            if (view.getInt() != FORMAT_VERSION) {
                return Optional.empty();
            }
            int expectedChecksum = view.getInt(CHECKSUM_BYTES);
            if (expectedChecksum != checksum(bytes)) {
                return Optional.empty();
            }
            RedoCheckpointLabel label = RedoCheckpointLabel.of(
                    Lsn.of(view.getLong(8)),
                    Lsn.of(view.getLong(16)),
                    view.getLong(24));
            return Optional.of(new Slot(slot, label));
        } catch (IOException e) {
            throw new RedoLogIoException("failed to read redo checkpoint slot from " + path, e);
        } catch (DatabaseValidationException e) {
            return Optional.empty();
        }
    }

    private static Slot newer(Slot a, Slot b) {
        long checkpointCompare = Long.compare(a.label().checkpointLsn().value(), b.label().checkpointLsn().value());
        if (checkpointCompare > 0) {
            return a;
        }
        if (checkpointCompare < 0) {
            return b;
        }
        return a.label().createdAtMillis() >= b.label().createdAtMillis() ? a : b;
    }

    private static ByteBuffer encode(RedoCheckpointLabel label) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putLong(label.checkpointLsn().value());
        buffer.putLong(label.currentLsnAtCheckpoint().value());
        buffer.putLong(label.createdAtMillis());
        buffer.putInt(checksum(buffer.array()));
        buffer.flip();
        return buffer;
    }

    private static int checksum(byte[] slotBytes) {
        CRC32 crc = new CRC32();
        crc.update(slotBytes, 0, CHECKSUM_BYTES);
        return (int) crc.getValue();
    }

    private record Slot(int slotIndex, RedoCheckpointLabel label) {
    }
}
