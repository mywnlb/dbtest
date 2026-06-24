package cn.zhangyis.db.storage.flush.doublewrite;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.flush.FlushWriteException;
import cn.zhangyis.db.storage.page.PageImageChecksum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * F1 doublewrite 文件仓储。它维护独立于 tablespace data file 的 append-only full-copy slots：
 * magic、format、page id、page LSN、page size、payload CRC 和整页 payload。
 *
 * <p>并发边界：slot append、force、scan 共用 ioLock 串行化，避免同一 FileChannel position 被并发读写打乱。
 * 该锁不回调 Buffer Pool、PageStore 或 Redo，防止 doublewrite 文件锁反向进入上层等待。
 */
public final class DoublewriteFileRepository implements AutoCloseable {

    private static final int MAGIC = 0x44574231; // "DWB1"
    private static final int FORMAT_VERSION = 1;
    private static final int SLOT_HEADER_BYTES = 36;

    /** 文件路径；仅用于诊断。 */
    private final Path path;
    /** 实例页大小；所有 slot payload 必须等长。 */
    private final PageSize pageSize;
    /** doublewrite 文件 channel。 */
    private final FileChannel channel;
    /** 保护 append/scan/force 的物理文件锁。 */
    private final ReentrantLock ioLock = new ReentrantLock();

    private DoublewriteFileRepository(Path path, PageSize pageSize, FileChannel channel) {
        this.path = path;
        this.pageSize = pageSize;
        this.channel = channel;
    }

    /**
     * 打开或创建 doublewrite 文件。
     *
     * @param path doublewrite 文件路径。
     * @param pageSize 页大小。
     * @return 已打开仓储。
     */
    public static DoublewriteFileRepository open(Path path, PageSize pageSize) {
        if (path == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite path/page size must not be null");
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new DoublewriteFileRepository(path, pageSize, channel);
        } catch (IOException e) {
            throw new FlushWriteException("failed to open doublewrite file: " + path, e);
        }
    }

    /**
     * 追加一个 full-copy slot。调用方负责在 data file write 前调用 {@link #force()}。
     *
     * @param snapshot 页镜像。
     */
    public void append(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        byte[] payload = snapshot.pageImage();
        requirePageImage(payload);
        ByteBuffer slot = ByteBuffer.allocate(SLOT_HEADER_BYTES + pageSize.bytes());
        slot.putInt(MAGIC);
        slot.putInt(FORMAT_VERSION);
        slot.putInt(snapshot.pageId().spaceId().value());
        slot.putLong(snapshot.pageId().pageNo().value());
        slot.putLong(snapshot.pageLsn().value());
        slot.putInt(pageSize.bytes());
        slot.putInt(crc32(payload));
        slot.put(payload);
        slot.flip();

        ioLock.lock();
        try {
            channel.position(channel.size());
            while (slot.hasRemaining()) {
                channel.write(slot);
            }
        } catch (IOException e) {
            throw new FlushWriteException("failed to append doublewrite slot: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * fsync doublewrite 文件。只有该方法返回后，FlushCoordinator 才能继续写 data file。
     */
    public void force() {
        ioLock.lock();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new FlushWriteException("failed to force doublewrite file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 查找目标页的最新有效 full-copy。有效表示 slot CRC 匹配且页镜像自身 checksum/trailer 校验通过。
     *
     * @param pageId 目标页。
     * @return 最新有效页副本。
     */
    public Optional<byte[]> latestCopy(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        ioLock.lock();
        try {
            Optional<byte[]> latest = Optional.empty();
            channel.position(0);
            while (true) {
                ByteBuffer header = ByteBuffer.allocate(SLOT_HEADER_BYTES);
                if (!readFullyOrTail(header)) {
                    break;
                }
                header.flip();
                int magic = header.getInt();
                if (magic != MAGIC) {
                    throw new FlushWriteException("doublewrite magic mismatch in " + path + ": " + magic);
                }
                int format = header.getInt();
                int space = header.getInt();
                long pageNo = header.getLong();
                header.getLong(); // pageLsn，F1 查询最新副本时不额外排序，文件 append 顺序即新旧顺序。
                int pageBytes = header.getInt();
                int expectedCrc = header.getInt();
                if (format != FORMAT_VERSION || pageBytes != pageSize.bytes()) {
                    throw new FlushWriteException("doublewrite slot format/page size mismatch in " + path);
                }
                ByteBuffer payload = ByteBuffer.allocate(pageBytes);
                if (!readFullyOrTail(payload)) {
                    break;
                }
                byte[] bytes = payload.array();
                if (crc32(bytes) == expectedCrc && PageImageChecksum.verify(bytes, pageSize)
                        && pageId.equals(PageId.of(SpaceId.of(space), PageNo.of(pageNo)))) {
                    latest = Optional.of(bytes.clone());
                }
            }
            return latest;
        } catch (IOException e) {
            throw new FlushWriteException("failed to scan doublewrite file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 枚举 doublewrite 文件中所有有效 slot 的去重页号（恢复期"待检查页列表"来源）。有效定义同 {@link #latestCopy}：
     * slot CRC 匹配且页镜像自身 checksum/trailer 校验通过；无效/尾部截断 slot 跳过。返回首见顺序的去重列表。
     *
     * @return 有有效副本的去重 {@link PageId} 列表。
     */
    public List<PageId> pageIds() {
        ioLock.lock();
        try {
            LinkedHashSet<PageId> ids = new LinkedHashSet<>();
            channel.position(0);
            while (true) {
                ByteBuffer header = ByteBuffer.allocate(SLOT_HEADER_BYTES);
                if (!readFullyOrTail(header)) {
                    break;
                }
                header.flip();
                int magic = header.getInt();
                if (magic != MAGIC) {
                    throw new FlushWriteException("doublewrite magic mismatch in " + path + ": " + magic);
                }
                int format = header.getInt();
                int space = header.getInt();
                long pageNo = header.getLong();
                header.getLong(); // pageLsn：枚举不需要，append 顺序即新旧顺序。
                int pageBytes = header.getInt();
                int expectedCrc = header.getInt();
                if (format != FORMAT_VERSION || pageBytes != pageSize.bytes()) {
                    throw new FlushWriteException("doublewrite slot format/page size mismatch in " + path);
                }
                ByteBuffer payload = ByteBuffer.allocate(pageBytes);
                if (!readFullyOrTail(payload)) {
                    break;
                }
                byte[] bytes = payload.array();
                if (crc32(bytes) == expectedCrc && PageImageChecksum.verify(bytes, pageSize)) {
                    ids.add(PageId.of(SpaceId.of(space), PageNo.of(pageNo)));
                }
            }
            return List.copyOf(ids);
        } catch (IOException e) {
            throw new FlushWriteException("failed to scan doublewrite file: " + path, e);
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
            throw new FlushWriteException("failed to close doublewrite file: " + path, e);
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

    private void requirePageImage(byte[] page) {
        if (page.length != pageSize.bytes()) {
            throw new DatabaseValidationException("doublewrite payload length must equal page size: expected "
                    + pageSize.bytes() + " got " + page.length);
        }
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
