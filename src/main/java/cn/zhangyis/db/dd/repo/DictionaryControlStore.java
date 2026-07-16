package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.dd.exception.DictionaryPersistenceException;
import cn.zhangyis.db.domain.SpaceId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/**
 * `mysql.dd.ctrl` 双槽高水位仓储。单把显式锁只保护本文件的 generation/counter 和 positional IO；锁内不访问
 * Buffer Pool、MTR、MDL 或用户表文件。每次 reserve 先写非当前槽并 force(true)，随后才发布内存快照，确保
 * 返回给 DDL 的 ID 已经不会在 crash 后复用。
 */
public final class DictionaryControlStore implements AutoCloseable {

    /** 每个副本独占一个 4 KiB 扇区/页，避免一次 torn write 同时破坏两代。 */
    public static final int SLOT_BYTES = 4096;

    private static final long MAGIC = 0x4D_49_4E_49_44_44_43_54L; // MINIDDCT
    private static final int FORMAT_VERSION = 1;
    private static final int CRC_OFFSET = SLOT_BYTES - Integer.BYTES;

    /** 串行化 generation 切换与关闭；不会跨越外部调用。 */
    private final ReentrantLock ioLock = new ReentrantLock();

    /** control 文件唯一 channel，由本对象拥有并在 close 释放。 */
    private final FileChannel channel;

    /** 诊断用稳定路径。 */
    private final Path path;

    /** 最近有效 generation；只在 ioLock 内替换，snapshot 通过同一锁复制。 */
    private DictionaryControlSnapshot current;

    private DictionaryControlStore(Path path, FileChannel channel, DictionaryControlSnapshot current) {
        this.path = path;
        this.channel = channel;
        this.current = current;
    }

    /**
     * 打开已有 control，或在文件不存在时创建初始 generation。初始 schema/version 从 2 开始，1 保留给
     * bootstrap mysql schema/version；table/index/DDL 从 1，用户 space 从配置的安全下界开始。
     */
    public static DictionaryControlStore openOrCreate(Path path, SpaceId dictionarySpaceId, int firstUserSpaceId) {
        validateOpenArguments(path, dictionarySpaceId);
        if (firstUserSpaceId <= dictionarySpaceId.value()) {
            throw new DatabaseValidationException("first user space id must be greater than dictionary space id");
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            boolean exists = Files.exists(path);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            if (exists && channel.size() > 0) {
                return openValidated(path, channel, dictionarySpaceId);
            }
            channel.truncate(SLOT_BYTES * 2L);
            DictionaryControlSnapshot initial = new DictionaryControlSnapshot(1, dictionarySpaceId,
                    2, 1, 1, firstUserSpaceId, 1, 2);
            writeSlot(channel, initial);
            channel.force(true);
            return new DictionaryControlStore(path, channel, initial);
        } catch (IOException e) {
            throw new DictionaryPersistenceException("open/create dictionary control failed: " + path, e);
        }
    }

    /** 打开已有 control；文件缺失、space id 不符或双槽均损坏都 fail-closed。 */
    public static DictionaryControlStore openExisting(Path path, SpaceId dictionarySpaceId) {
        validateOpenArguments(path, dictionarySpaceId);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            return openValidated(path, channel, dictionarySpaceId);
        } catch (IOException e) {
            throw new DictionaryPersistenceException("open dictionary control failed: " + path, e);
        }
    }

    private static DictionaryControlStore openValidated(Path path, FileChannel channel, SpaceId expectedSpaceId) {
        try {
            List<DictionaryControlSnapshot> candidates = new ArrayList<>(2);
            decodeSlot(channel, 0).ifPresent(candidates::add);
            decodeSlot(channel, 1).ifPresent(candidates::add);
            DictionaryControlSnapshot latest = candidates.stream()
                    .max(Comparator.comparingLong(DictionaryControlSnapshot::generation))
                    .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                            "both dictionary control slots are invalid: " + path));
            if (!latest.dictionarySpaceId().equals(expectedSpaceId)) {
                throw new DictionaryCatalogCorruptionException("dictionary control space id mismatch: expected="
                        + expectedSpaceId.value() + ", actual=" + latest.dictionarySpaceId().value());
            }
            return new DictionaryControlStore(path, channel, latest);
        } catch (RuntimeException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /**
     * 原子预留各身份区间。溢出校验在任何写盘前完成；写槽和 force 成功后才替换 current，因此异常返回时
     * 调用方绝不能使用 allocation，内存也不会声称未 durable 的 generation 已生效。
     */
    public DictionaryIdAllocation reserve(DictionaryIdRequest request) {
        if (request == null) {
            throw new DatabaseValidationException("dictionary id request must not be null");
        }
        ioLock.lock();
        try {
            DictionaryControlSnapshot before = current;
            long schema = advance(before.nextSchemaId(), request.schemaCount(), "schema");
            long table = advance(before.nextTableId(), request.tableCount(), "table");
            long index = advance(before.nextIndexId(), request.indexCount(), "index");
            long space = advance(before.nextSpaceId(), request.spaceCount(), "space");
            long ddl = advance(before.nextDdlId(), request.ddlCount(), "ddl");
            long version = advance(before.nextDictionaryVersion(), request.versionCount(), "dictionary version");
            if (space > Integer.MAX_VALUE) {
                throw new DatabaseValidationException("dictionary tablespace id range exhausted");
            }
            DictionaryControlSnapshot next = new DictionaryControlSnapshot(
                    advance(before.generation(), 1, "control generation"), before.dictionarySpaceId(),
                    schema, table, index, space, ddl, version);
            try {
                writeSlot(channel, next);
                channel.force(true);
            } catch (IOException e) {
                throw new DictionaryPersistenceException("reserve dictionary ids failed: " + path, e);
            }
            current = next;
            return new DictionaryIdAllocation(
                    request.schemaCount() == 0 ? 0 : before.nextSchemaId(),
                    request.tableCount() == 0 ? 0 : before.nextTableId(),
                    request.indexCount() == 0 ? 0 : before.nextIndexId(),
                    request.spaceCount() == 0 ? 0 : Math.toIntExact(before.nextSpaceId()),
                    request.ddlCount() == 0 ? 0 : before.nextDdlId(),
                    request.versionCount() == 0 ? 0 : before.nextDictionaryVersion(),
                    request, next.generation());
        } finally {
            ioLock.unlock();
        }
    }

    /** 返回最近 durable 槽的不可变快照。 */
    public DictionaryControlSnapshot snapshot() {
        ioLock.lock();
        try {
            return current;
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
            throw new DictionaryPersistenceException("close dictionary control failed: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private static long advance(long current, int count, String kind) {
        try {
            return Math.addExact(current, count);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("dictionary " + kind + " id range overflow", overflow);
        }
    }

    private static void validateOpenArguments(Path path, SpaceId dictionarySpaceId) {
        if (path == null || dictionarySpaceId == null) {
            throw new DatabaseValidationException("dictionary control path/space id must not be null");
        }
    }

    private static void writeSlot(FileChannel channel, DictionaryControlSnapshot snapshot) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putLong(snapshot.generation());
        buffer.putInt(snapshot.dictionarySpaceId().value());
        buffer.putLong(snapshot.nextSchemaId());
        buffer.putLong(snapshot.nextTableId());
        buffer.putLong(snapshot.nextIndexId());
        buffer.putLong(snapshot.nextSpaceId());
        buffer.putLong(snapshot.nextDdlId());
        buffer.putLong(snapshot.nextDictionaryVersion());
        int crc = crc(buffer.array(), CRC_OFFSET);
        buffer.position(CRC_OFFSET);
        buffer.putInt(crc);
        buffer.flip();
        writeFully(channel, buffer, slotOffset(snapshot.generation()));
    }

    private static java.util.Optional<DictionaryControlSnapshot> decodeSlot(FileChannel channel, int slot) {
        try {
            if (channel.size() < (slot + 1L) * SLOT_BYTES) {
                return java.util.Optional.empty();
            }
            ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, buffer, slot * (long) SLOT_BYTES);
            byte[] bytes = buffer.array();
            int storedCrc = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(CRC_OFFSET);
            if (storedCrc != crc(bytes, CRC_OFFSET)) {
                return java.util.Optional.empty();
            }
            buffer.flip();
            if (buffer.getLong() != MAGIC || buffer.getInt() != FORMAT_VERSION) {
                return java.util.Optional.empty();
            }
            long generation = buffer.getLong();
            int spaceId = buffer.getInt();
            long nextSchema = buffer.getLong();
            long nextTable = buffer.getLong();
            long nextIndex = buffer.getLong();
            long nextSpace = buffer.getLong();
            long nextDdl = buffer.getLong();
            long nextVersion = buffer.getLong();
            if (generation <= 0 || spaceId < 0 || nextSchema <= 0 || nextTable <= 0 || nextIndex <= 0
                    || nextSpace <= 0 || nextSpace > Integer.MAX_VALUE || nextDdl <= 0 || nextVersion <= 0) {
                return java.util.Optional.empty();
            }
            return java.util.Optional.of(new DictionaryControlSnapshot(generation, SpaceId.of(spaceId),
                    nextSchema, nextTable, nextIndex, nextSpace, nextDdl, nextVersion));
        } catch (IOException | RuntimeException ignored) {
            return java.util.Optional.empty();
        }
    }

    private static long slotOffset(long generation) {
        return Math.floorMod(generation, 2) * (long) SLOT_BYTES;
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
                throw new IOException("dictionary control positional write made no progress");
            }
            position += written;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in dictionary control slot");
            }
            if (read == 0) {
                throw new IOException("dictionary control positional read made no progress");
            }
            position += read;
        }
    }
}
