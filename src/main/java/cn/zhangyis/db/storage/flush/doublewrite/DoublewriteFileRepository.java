package cn.zhangyis.db.storage.flush.doublewrite;

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
 * F1 doublewrite 文件仓储。它维护独立于 tablespace data file 的有界 full-copy slots：
 * magic、format、page id、page LSN、page size、payload CRC 和整页 payload。
 *
 * <p>并发边界：slot append、force、scan 共用 ioLock 串行化，避免同一 FileChannel position 被并发读写打乱。
 * 该锁不回调 Buffer Pool、PageStore 或 Redo，防止 doublewrite 文件锁反向进入上层等待。
 *
 * <p>0.5 简化点：slot 元数据不额外持久化空闲链表；进程内用 in-flight 标记避免覆盖尚未完成 data file force
 * 的副本，已完成 slot 可循环复用。崩溃恢复时所有文件内有效 slot 都按 pageLSN 扫描，旧副本直到被后续 slot
 * 写入覆盖前都可用于 torn-page 修复。
 */
public final class DoublewriteFileRepository implements AutoCloseable {

    private static final int MAGIC = 0x44574231; // "DWB1"
    private static final int LEGACY_FULL_COPY_FORMAT_VERSION = 1;
    private static final int FORMAT_VERSION = 2;
    private static final int SLOT_HEADER_BYTES = 36;
    private static final int DETECT_ONLY_METADATA_BYTES = Integer.BYTES + Long.BYTES + Integer.BYTES;
    private static final int DEFAULT_SLOT_COUNT = 1024;

    /** 文件路径；仅用于诊断。 */
    private final Path path;
    /** 实例页大小；所有 slot payload 必须等长。 */
    private final PageSize pageSize;
    /** 固定 slot 数量。达到上限后只复用已完成 slot，避免 doublewrite 文件无界增长。 */
    private final int slotCount;
    /** doublewrite 文件 channel。 */
    private final FileChannel channel;
    /** 保护 append/scan/force 的物理文件锁。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 进程内 in-flight 标记；true 表示该 slot 的 data file write 尚未通过 afterDataFileWrite 完成。 */
    private final boolean[] inFlight;
    /** 与 inFlight 对齐的页标识，用于 afterDataFileWrite/abort 清理对应 slot。 */
    private final SlotReservation[] reservations;
    /** 下一次分配 slot 的循环游标；只在 ioLock 下读写。 */
    private int nextSlot;

    private DoublewriteFileRepository(Path path, PageSize pageSize, int slotCount, FileChannel channel) {
        this.path = path;
        this.pageSize = pageSize;
        this.slotCount = slotCount;
        this.channel = channel;
        this.inFlight = new boolean[slotCount];
        this.reservations = new SlotReservation[slotCount];
    }

    /**
     * 打开或创建 doublewrite 文件。
     *
     * @param path doublewrite 文件路径。
     * @param pageSize 页大小。
     * @return 已打开仓储。
     */
    public static DoublewriteFileRepository open(Path path, PageSize pageSize) {
        return open(path, pageSize, DEFAULT_SLOT_COUNT);
    }

    /**
     * 打开或创建有界 doublewrite 文件。测试可传入很小的 slotCount 验证复用行为；生产默认使用
     * {@link #DEFAULT_SLOT_COUNT}，使文件最多增长到 {@code slotCount * slotSize}。
     *
     * @param path doublewrite 文件路径。
     * @param pageSize 页大小。
     * @param slotCount 固定 slot 数量，必须大于 0。
     * @return 已打开仓储。
     */
    public static DoublewriteFileRepository open(Path path, PageSize pageSize, int slotCount) {
        if (path == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite path/page size must not be null");
        }
        if (slotCount <= 0) {
            throw new DatabaseValidationException("doublewrite slot count must be positive: " + slotCount);
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new DoublewriteFileRepository(path, pageSize, slotCount, channel);
        } catch (IOException e) {
            throw new FlushWriteException("failed to open doublewrite file: " + path, e);
        }
    }

    /**
     * 写入一个 full-copy slot。调用方负责在 data file write 前调用 {@link #force()}，并在 data file force 成功或
     * 放弃写 data file 时调用 {@link #releaseSlot(FlushPageSnapshot)} 释放 in-flight 标记。
     *
     * @param snapshot 页镜像。
     */
    public void append(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        appendBatch(DoublewriteBatch.single(snapshot));
    }

    /**
     * 写入 detect-only metadata slot。该 slot 只用于恢复期发现可疑页，不能作为完整页副本写回 data file。
     *
     * @param snapshot 页镜像快照；仓储只持久化 pageId/pageLSN/page checksum 摘要。
     */
    public void appendDetectOnly(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        ioLock.lock();
        int startSlot = -1;
        try {
            DoublewriteBatch batch = DoublewriteBatch.single(snapshot);
            startSlot = reserveBatch(batch);
            ByteBuffer slot = encodeDetectOnlySlot(snapshot);
            channel.position(slotOffset(startSlot));
            while (slot.hasRemaining()) {
                channel.write(slot);
            }
        } catch (IOException e) {
            if (startSlot >= 0) {
                clearReservationRange(startSlot, 1);
            }
            throw new FlushWriteException("failed to append detect-only doublewrite slot: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 在同一个 doublewrite 文件临界区内顺序写入一个批次。批次需要连续 slot；如果当前循环游标后方没有足够
     * 连续空闲 slot，会从文件头重新寻找连续空闲区。找不到时拒绝本批而不是拆批写入，避免一个 batch 横跨
     * 多个不相邻区域，后续接入真正 DoublewriteService 时可由上游按容量拆分。
     *
     * @param batch 待写入的 full-copy 批次。
     */
    public void appendBatch(DoublewriteBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("doublewrite batch must not be null");
        }
        ioLock.lock();
        int startSlot = -1;
        try {
            startSlot = reserveBatch(batch);
            for (int i = 0; i < batch.snapshots().size(); i++) {
                FlushPageSnapshot snapshot = batch.snapshots().get(i);
                ByteBuffer slot = encodeSlot(snapshot);
                channel.position(slotOffset(startSlot + i));
                while (slot.hasRemaining()) {
                    channel.write(slot);
                }
            }
        } catch (IOException e) {
            if (startSlot >= 0) {
                clearReservationRange(startSlot, batch.size());
            }
            throw new FlushWriteException("failed to append doublewrite batch: " + path, e);
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
     * 释放与快照匹配的 in-flight slot。该方法只释放进程内占用，不擦除磁盘副本；磁盘上的旧 full-copy 在被后续
     * slot 写入覆盖前仍可供恢复扫描使用。这样既能避免文件无界增长，又不会在 data file 刚写完时主动删除
     * torn-page 修复所需的最后副本。
     *
     * @param snapshot 已完成或已放弃 data file write 的页镜像。
     */
    public void releaseSlot(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        releaseBatch(DoublewriteBatch.single(snapshot));
    }

    /**
     * 释放批次内所有匹配的 in-flight slot。该方法只清理进程内占用，不清除磁盘副本；如果 data file write
     * 已经完成，副本继续留给未来 crash recovery 使用，直到后续 batch 复用该 slot 覆盖。
     *
     * @param batch 已完成或已放弃 data file write 的批次。
     */
    public void releaseBatch(DoublewriteBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("doublewrite batch must not be null");
        }
        ioLock.lock();
        try {
            for (FlushPageSnapshot snapshot : batch.snapshots()) {
                for (int i = 0; i < reservations.length; i++) {
                    SlotReservation reservation = reservations[i];
                    if (reservation != null && reservation.matches(snapshot)) {
                        clearReservation(i);
                    }
                }
            }
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
            long latestPageLsn = Long.MIN_VALUE;
            long size = channel.size();
            for (long position = 0; position + SLOT_HEADER_BYTES <= size; position += slotSize()) {
                ValidSlot slot = readValidSlotAt(position);
                if (slot != null && slot.kind() == DoublewriteSlotKind.FULL_COPY
                        && pageId.equals(slot.pageId()) && slot.pageLsn() >= latestPageLsn) {
                    latestPageLsn = slot.pageLsn();
                    byte[] bytes = slot.payload();
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
     * 枚举 doublewrite 文件中所有有效 slot 的去重页号（恢复期"待检查页列表"来源）。full-copy 有效要求页镜像自身
     * checksum/trailer 通过；detect-only 有效要求 metadata payload CRC 通过。返回首见顺序的去重列表。
     *
     * @return 有有效副本的去重 {@link PageId} 列表。
     */
    public List<PageId> pageIds() {
        ioLock.lock();
        try {
            LinkedHashSet<PageId> ids = new LinkedHashSet<>();
            long size = channel.size();
            for (long position = 0; position + SLOT_HEADER_BYTES <= size; position += slotSize()) {
                ValidSlot slot = readValidSlotAt(position);
                if (slot != null) {
                    ids.add(slot.pageId());
                }
            }
            return List.copyOf(ids);
        } catch (IOException e) {
            throw new FlushWriteException("failed to scan doublewrite file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 枚举 doublewrite 文件中的有效 slot 摘要。该方法是恢复诊断入口：调用方可区分 full-copy 与 detect-only，
     * 避免把 detect-only metadata 误当作可写回 data file 的完整页副本。
     *
     * @return 有效 slot 摘要列表，按文件位置顺序返回。
     */
    public List<DoublewriteSlotEntry> scanEntries() {
        ioLock.lock();
        try {
            java.util.ArrayList<DoublewriteSlotEntry> entries = new java.util.ArrayList<>();
            long size = channel.size();
            for (long position = 0; position + SLOT_HEADER_BYTES <= size; position += slotSize()) {
                ValidSlot slot = readValidSlotAt(position);
                if (slot != null) {
                    entries.add(new DoublewriteSlotEntry(slot.pageId(), slot.pageLsn(), slot.kind(), slot.checksum()));
                }
            }
            return List.copyOf(entries);
        } catch (IOException e) {
            throw new FlushWriteException("failed to scan doublewrite entries: " + path, e);
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

    private int reserveBatch(DoublewriteBatch batch) {
        if (batch.size() > slotCount) {
            throw new FlushWriteException("doublewrite batch exceeds slot capacity: batchSize=" + batch.size()
                    + ", slotCount=" + slotCount);
        }
        int start = findConsecutiveFreeSlots(batch.size());
        if (start < 0) {
            throw new FlushWriteException("no reusable consecutive doublewrite slots available: path=" + path
                    + ", slotCount=" + slotCount + ", batchSize=" + batch.size());
        }
        for (int i = 0; i < batch.snapshots().size(); i++) {
            FlushPageSnapshot snapshot = batch.snapshots().get(i);
            requirePageImage(snapshot.pageImage());
            int slotIndex = start + i;
            inFlight[slotIndex] = true;
            reservations[slotIndex] = new SlotReservation(snapshot.pageId(), snapshot.pageLsn().value());
        }
        nextSlot = (start + batch.size()) % slotCount;
        return start;
    }

    private int findConsecutiveFreeSlots(int batchSize) {
        int start = findConsecutiveFreeSlotsInRange(nextSlot, slotCount, batchSize);
        if (start >= 0) {
            return start;
        }
        return findConsecutiveFreeSlotsInRange(0, nextSlot, batchSize);
    }

    private int findConsecutiveFreeSlotsInRange(int fromInclusive, int toExclusive, int batchSize) {
        if (toExclusive - fromInclusive < batchSize) {
            return -1;
        }
        int maxStart = toExclusive - batchSize;
        for (int start = fromInclusive; start <= maxStart; start++) {
            boolean free = true;
            for (int offset = 0; offset < batchSize; offset++) {
                if (inFlight[start + offset]) {
                    free = false;
                    break;
                }
            }
            if (free) {
                return start;
            }
        }
        return -1;
    }

    private void clearReservation(int slotIndex) {
        inFlight[slotIndex] = false;
        reservations[slotIndex] = null;
    }

    private void clearReservationRange(int startSlot, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            clearReservation(startSlot + i);
        }
    }

    private long slotOffset(int slotIndex) {
        return (long) slotIndex * slotSize();
    }

    private int slotSize() {
        return SLOT_HEADER_BYTES + pageSize.bytes();
    }

    private ByteBuffer encodeSlot(FlushPageSnapshot snapshot) {
        byte[] payload = snapshot.pageImage();
        requirePageImage(payload);
        ByteBuffer slot = ByteBuffer.allocate(slotSize());
        slot.putInt(MAGIC);
        slot.putInt(FORMAT_VERSION);
        slot.putInt(snapshot.pageId().spaceId().value());
        slot.putLong(snapshot.pageId().pageNo().value());
        slot.putLong(snapshot.pageLsn().value());
        slot.putInt(pageSize.bytes());
        slot.putInt(crc32(payload));
        slot.put(payload);
        slot.flip();
        return slot;
    }

    private ByteBuffer encodeDetectOnlySlot(FlushPageSnapshot snapshot) {
        byte[] source = snapshot.pageImage();
        requirePageImage(source);
        byte[] payload = new byte[pageSize.bytes()];
        ByteBuffer metadata = ByteBuffer.wrap(payload);
        metadata.putInt(DETECT_ONLY_METADATA_BYTES);
        metadata.putLong(snapshot.pageLsn().value());
        metadata.putInt(crc32(source));
        ByteBuffer slot = ByteBuffer.allocate(slotSize());
        slot.putInt(MAGIC);
        slot.putInt(FORMAT_VERSION);
        slot.putInt(snapshot.pageId().spaceId().value());
        slot.putLong(snapshot.pageId().pageNo().value());
        slot.putLong(snapshot.pageLsn().value());
        slot.putInt(pageSize.bytes());
        slot.putInt(crc32(payload));
        slot.put(payload);
        slot.flip();
        return slot;
    }

    private ValidSlot readValidSlotAt(long position) throws IOException {
        channel.position(position);
        ByteBuffer header = ByteBuffer.allocate(SLOT_HEADER_BYTES);
        if (!readFullyOrTail(header)) {
            return null;
        }
        header.flip();
        int magic = header.getInt();
        if (magic != MAGIC) {
            return null;
        }
        int format = header.getInt();
        int space = header.getInt();
        long pageNo = header.getLong();
        long pageLsn = header.getLong();
        int pageBytes = header.getInt();
        int expectedCrc = header.getInt();
        if ((format != LEGACY_FULL_COPY_FORMAT_VERSION && format != FORMAT_VERSION)
                || pageBytes != pageSize.bytes()) {
            return null;
        }
        ByteBuffer payload = ByteBuffer.allocate(pageBytes);
        if (!readFullyOrTail(payload)) {
            return null;
        }
        byte[] bytes = payload.array();
        if (crc32(bytes) != expectedCrc) {
            return null;
        }
        if (format == LEGACY_FULL_COPY_FORMAT_VERSION) {
            if (!PageImageChecksum.verify(bytes, pageSize)) {
                return null;
            }
            return new ValidSlot(PageId.of(SpaceId.of(space), PageNo.of(pageNo)), pageLsn,
                    DoublewriteSlotKind.FULL_COPY, expectedCrc, bytes);
        }
        if (PageImageChecksum.verify(bytes, pageSize)) {
            return new ValidSlot(PageId.of(SpaceId.of(space), PageNo.of(pageNo)), pageLsn,
                    DoublewriteSlotKind.FULL_COPY, expectedCrc, bytes);
        }
        if (!isValidDetectOnlyPayload(bytes, pageLsn)) {
            return null;
        }
        return new ValidSlot(PageId.of(SpaceId.of(space), PageNo.of(pageNo)), pageLsn,
                DoublewriteSlotKind.DETECT_ONLY_METADATA, expectedCrc, bytes);
    }

    private boolean isValidDetectOnlyPayload(byte[] bytes, long pageLsn) {
        ByteBuffer metadata = ByteBuffer.wrap(bytes);
        int metadataBytes = metadata.getInt();
        long metadataLsn = metadata.getLong();
        metadata.getInt();
        return metadataBytes == DETECT_ONLY_METADATA_BYTES && metadataLsn == pageLsn;
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

    private record SlotReservation(PageId pageId, long pageLsn) {

        private boolean matches(FlushPageSnapshot snapshot) {
            return pageId.equals(snapshot.pageId()) && pageLsn == snapshot.pageLsn().value();
        }
    }

    private record ValidSlot(PageId pageId, long pageLsn, DoublewriteSlotKind kind, int checksum, byte[] payload) {
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
