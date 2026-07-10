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
 * Redo checkpoint control v2 双槽仓储。两个 52B slot 分别位于独立 4 KiB 文件页，避免一次 torn sector/page
 * write 同时破坏两份 label；写槽 force 成功后才切换 nextSlot/generation。
 *
 * <p>slot 同时绑定 redo data format version。已知 control v1 明确 fail-closed，不能退化为 checkpoint 0 后
 * 与 LogBlock v1 混用。READ_ONLY_VALIDATE 使用只读 channel，不创建、预分配或写槽。
 */
public final class RedoCheckpointStore implements AutoCloseable {

    /** slot magic：ASCII "RCP2"，沿用稳定文件身份。 */
    static final int MAGIC = 0x52435032;
    /** control v2：增加 redo format/generation，并物理分隔双槽。 */
    private static final int CONTROL_FORMAT_VERSION = 2;
    /** magic/controlVersion/redoVersion/reserved/checkpoint/current/time/generation/crc。 */
    static final int SLOT_BYTES = 52;
    /** 双槽起始偏移跨度。 */
    static final int SLOT_STRIDE_BYTES = 4 * 1024;
    /** 固定双槽文件长度。 */
    private static final long FILE_BYTES = 2L * SLOT_STRIDE_BYTES;
    /** checksum 覆盖 checksum 字段之前的全部 slot bytes。 */
    private static final int CHECKSUM_BYTES = SLOT_BYTES - Integer.BYTES;

    /** control 路径，用于格式/IO 诊断。 */
    private final Path path;
    /** positional IO channel；只读实例没有 WRITE capability。 */
    private final FileChannel channel;
    /** 是否允许预分配和写槽。 */
    private final boolean writable;
    /** 保护 slot 选择、读写、force 与关闭。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 下一次覆盖槽编号，由最新有效 generation 恢复。 */
    private int nextSlot;
    /** 下一次写入 generation；只在 force 成功后递增。 */
    private long nextGeneration;

    private RedoCheckpointStore(Path path, FileChannel channel, boolean writable) throws IOException {
        this.path = path;
        this.channel = channel;
        this.writable = writable;
        rejectLegacyControl();
        if (channel.size() > FILE_BYTES) {
            throw new RedoLogCorruptedException(
                    "redo checkpoint control exceeds fixed v2 size: " + channel.size() + " > " + FILE_BYTES);
        }
        if (writable) {
            ensureFixedFileSize();
        }
        Optional<Slot> latest = latestSlot();
        nextSlot = latest.map(slot -> 1 - slot.slotIndex()).orElse(0);
        nextGeneration = latest.map(slot -> incrementGeneration(slot.generation())).orElse(1L);
    }

    /** 打开或创建 writable control v2；旧 v1 文件在任何预分配/写入前拒绝。 */
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
            return construct(path, channel, true);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open redo checkpoint control file: " + path, e);
        }
    }

    /** 只读打开 existing control v2；路径缺失时不创建文件。 */
    public static RedoCheckpointStore openReadOnly(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("read-only redo checkpoint path must not be null");
        }
        try {
            return construct(path, FileChannel.open(path, StandardOpenOption.READ), false);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open read-only redo checkpoint control: " + path, e);
        }
    }

    private static RedoCheckpointStore construct(Path path, FileChannel channel, boolean writable) throws IOException {
        try {
            return new RedoCheckpointStore(path, channel, writable);
        } catch (RuntimeException | IOException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** 读取 checkpoint 最大、同 checkpoint 下 generation 最大的有效 label；双槽均无效时安全回到当前格式初始值。 */
    public RedoCheckpointLabel readLatest() {
        ioLock.lock();
        try {
            return latestSlot().map(Slot::label).orElseGet(RedoCheckpointLabel::initial);
        } finally {
            ioLock.unlock();
        }
    }

    /** 写入并 force 一个当前 redo data 格式的 checkpoint label。 */
    public void write(RedoCheckpointLabel label) {
        if (label == null) {
            throw new DatabaseValidationException("redo checkpoint label must not be null");
        }
        requireWritable();
        if (label.redoFormatVersion() != RedoLogBlockCodec.FORMAT_VERSION) {
            throw new RedoLogFormatException("cannot persist checkpoint for redo format "
                    + label.redoFormatVersion() + "; current=" + RedoLogBlockCodec.FORMAT_VERSION);
        }
        ioLock.lock();
        try {
            ByteBuffer encoded = encode(label, nextGeneration);
            long offset = slotOffset(nextSlot);
            writeFullyAt(offset, encoded);
            channel.force(true);
            nextSlot = 1 - nextSlot;
            nextGeneration = incrementGeneration(nextGeneration);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to write redo checkpoint label to " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 关闭 control channel；失败保留底层 cause。 */
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
                if (read < 0 || read == 0) {
                    return Optional.empty();
                }
            }
            byte[] bytes = buffer.array();
            ByteBuffer view = ByteBuffer.wrap(bytes);
            int magic = view.getInt();
            if (magic != MAGIC) {
                return Optional.empty();
            }
            // version/format 等字段只有在整槽 CRC 通过后才可信。若先解释 version，单字节 torn write 会被
            // 错报为“不支持格式”，并阻止另一个有效槽接管恢复。
            if (view.getInt(CHECKSUM_BYTES) != checksum(bytes)) {
                return Optional.empty();
            }
            int controlVersion = view.getInt();
            if (controlVersion != CONTROL_FORMAT_VERSION) {
                throw new RedoLogFormatException(
                        "unsupported redo checkpoint control version " + controlVersion + ": " + path);
            }
            int redoFormatVersion = view.getInt();
            int reserved = view.getInt();
            if (reserved != 0) {
                throw new RedoLogCorruptedException("redo checkpoint reserved field is non-zero: " + path);
            }
            long generation = view.getLong(40);
            if (generation <= 0) {
                throw new RedoLogCorruptedException("redo checkpoint generation is invalid: " + generation);
            }
            RedoCheckpointLabel label = RedoCheckpointLabel.decoded(
                    Lsn.of(view.getLong(16)), Lsn.of(view.getLong(24)),
                    view.getLong(32), redoFormatVersion);
            return Optional.of(new Slot(slotIndex, label, generation));
        } catch (IOException e) {
            throw new RedoLogIoException("failed to read redo checkpoint slot from " + path, e);
        } catch (DatabaseValidationException e) {
            throw new RedoLogCorruptedException(
                    "redo checkpoint slot contains checksum-valid invalid fields: " + path, e);
        }
    }

    private static Slot newer(Slot first, Slot second) {
        int checkpointOrder = Long.compare(
                first.label().checkpointLsn().value(), second.label().checkpointLsn().value());
        if (checkpointOrder != 0) {
            return checkpointOrder > 0 ? first : second;
        }
        return first.generation() >= second.generation() ? first : second;
    }

    private static ByteBuffer encode(RedoCheckpointLabel label, long generation) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(CONTROL_FORMAT_VERSION);
        buffer.putInt(label.redoFormatVersion());
        buffer.putInt(0);
        buffer.putLong(label.checkpointLsn().value());
        buffer.putLong(label.currentLsnAtCheckpoint().value());
        buffer.putLong(label.createdAtMillis());
        buffer.putLong(generation);
        buffer.putInt(checksum(buffer.array()));
        buffer.flip();
        return buffer;
    }

    /** 在扩展文件前读取首 8B，确保已知 v1 不会被 v2 预分配覆盖。 */
    private void rejectLegacyControl() throws IOException {
        if (channel.size() < 8) {
            return;
        }
        ByteBuffer prefix = ByteBuffer.allocate(8);
        while (prefix.hasRemaining()) {
            int read = channel.read(prefix, prefix.position());
            if (read <= 0) {
                return;
            }
        }
        prefix.flip();
        if (prefix.getInt() == MAGIC && prefix.getInt() == 1) {
            throw new RedoLogFormatException("legacy redo checkpoint control v1 is not supported: " + path);
        }
    }

    /** 预分配两个独立 4 KiB 页；零 padding 不会形成合法 slot。 */
    private void ensureFixedFileSize() throws IOException {
        if (channel.size() >= FILE_BYTES) {
            return;
        }
        writeFullyAt(FILE_BYTES - 1, ByteBuffer.wrap(new byte[]{0}));
        channel.force(true);
    }

    /** positional write 必须持续前进；零进度不能在 control 临界区无限自旋。 */
    private void writeFullyAt(long offset, ByteBuffer source) throws IOException {
        long position = offset;
        while (source.hasRemaining()) {
            int written = channel.write(source, position);
            if (written <= 0) {
                throw new RedoLogIoException("zero-progress write while updating redo checkpoint: " + path);
            }
            position += written;
        }
    }

    private void requireWritable() {
        if (!writable) {
            throw new DatabaseValidationException("read-only redo checkpoint store cannot write: " + path);
        }
    }

    private static long slotOffset(int slotIndex) {
        return (long) slotIndex * SLOT_STRIDE_BYTES;
    }

    private static int checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, CHECKSUM_BYTES);
        return (int) crc.getValue();
    }

    private static long incrementGeneration(long generation) {
        try {
            return Math.addExact(generation, 1L);
        } catch (ArithmeticException e) {
            throw new RedoLogCorruptedException("redo checkpoint generation overflow: " + generation, e);
        }
    }

    private record Slot(int slotIndex, RedoCheckpointLabel label, long generation) {
    }
}
