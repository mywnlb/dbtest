package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 每个表空间一个的物理数据文件单元。封装打开的 FileChannel、生命周期闩(#1)、文件大小锁(#3) 和权威物理大小，
 * 真正做 positional 整页读写与文件扩展。它是纯物理视角：按 pageNo 计算文件偏移，不解析页内容、不算 checksum、
 * 不懂 segment/record（设计 §10）。
 *
 * <p>并发：read/write 持 Lifecycle(S) 可并发；autoExtend 持 Lifecycle(S)+FileSize(X)；close 持 Lifecycle(X)。
 * currentSizeInPages 用 volatile 发布，保证扩展后零填充的新页"发布前对读不可见"。同页并发写不在本层串行化，
 * 由上层 Buffer Pool page latch 负责（设计 §8.1）。
 *
 * <p>简化点：单文件；句柄不替换（#2 预留）。
 */
final class DataFileHandle implements AutoCloseable {

    /**
     * 所属表空间编号；仅用于诊断与构造 PageId 计算偏移。
     */
    private final SpaceId spaceId;

    /**
     * 数据文件路径；诊断用，IO 走已打开的 channel。
     */
    private final Path path;

    /**
     * 实例级页大小；决定偏移换算与越界单位。
     */
    private final PageSize pageSize;

    /**
     * 已打开的文件通道；整生命周期持有，由生命周期闩保护其使用与关闭。
     */
    private final FileChannel channel;

    /**
     * #1 生命周期闩。
     */
    private final TablespaceLifecycleLatch lifecycleLatch = new TablespaceLifecycleLatch();

    /**
     * #3 文件大小锁。
     */
    private final FileSizeLock fileSizeLock = new FileSizeLock();

    /**
     * 权威物理大小（页数）。读路径读该 volatile 快照做越界检查；autoExtend 在 fileSizeLock 下零填充后再发布。
     */
    private volatile long currentSizeInPages;

    /**
     * 关闭标志。close() 在 Lifecycle(X) 下置位；IO 路径在 Lifecycle(S) 下检查，防止使用已关闭 channel。
     */
    private volatile boolean closed;

    private DataFileHandle(SpaceId spaceId, Path path, PageSize pageSize, FileChannel channel, long currentSizeInPages) {
        this.spaceId = spaceId;
        this.path = path;
        this.pageSize = pageSize;
        this.channel = channel;
        this.currentSizeInPages = currentSizeInPages;
    }

    /**
     * 创建新数据文件并零填充 initialSizeInPages 页。文件必须不存在，否则视为重复创建错误。
     *
     * @param spaceId 表空间编号。
     * @param path 文件路径。
     * @param pageSize 页大小。
     * @param initialSizeInPages 初始页数（非负）。
     * @return 已登记物理大小的句柄。
     */
    static DataFileHandle create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
        validate(spaceId, path, pageSize);
        if (initialSizeInPages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        if (Files.exists(path)) {
            throw new DataFilePhysicalException("data file already exists: " + path);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long pages = initialSizeInPages.value();
            zeroFill(channel, 0, pages, pageSize.bytes());
            return new DataFileHandle(spaceId, path, pageSize, channel, pages);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new DataFilePhysicalException("create data file failed: " + path, e);
        }
    }

    /**
     * 打开已存在数据文件，size 由文件长度推导。文件必须存在且整页对齐。
     *
     * @param spaceId 表空间编号。
     * @param path 文件路径。
     * @param pageSize 页大小。
     * @return 已登记物理大小的句柄。
     */
    static DataFileHandle open(SpaceId spaceId, Path path, PageSize pageSize) {
        validate(spaceId, path, pageSize);
        if (!Files.exists(path)) {
            throw new DataFilePhysicalException("data file not found: " + path);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long length = channel.size();
            int pageBytes = pageSize.bytes();
            if (length % pageBytes != 0) {
                closeQuietly(channel);
                throw new DataFileCorruptedException("data file not page-aligned: " + path + " length=" + length);
            }
            return new DataFileHandle(spaceId, path, pageSize, channel, length / pageBytes);
        } catch (IOException e) {
            closeQuietly(channel);
            throw new DataFilePhysicalException("open data file failed: " + path, e);
        }
    }

    /**
     * 读取整页到 dst。持 Lifecycle(S) → 检关闭 → volatile size 越界检查 → positional 读满 pageSize 字节。
     *
     * @param pageNo 表空间内页号。
     * @param dst 目标缓冲，remaining 必须 == pageSize。
     */
    void readPage(PageNo pageNo, ByteBuffer dst) {
        requirePageSized(dst);
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            ensureOpen();
            long offset = boundedOffset(pageNo);
            readFully(dst, offset);
        }
    }

    /**
     * 写入整页。持 Lifecycle(S) → 检关闭 → 越界检查 → positional 写满 pageSize 字节。
     * 同页并发写的互斥由上层 page latch 负责，本层不串行化。
     *
     * @param pageNo 表空间内页号。
     * @param src 源缓冲，remaining 必须 == pageSize。
     */
    void writePage(PageNo pageNo, ByteBuffer src) {
        requirePageSized(src);
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            ensureOpen();
            long offset = boundedOffset(pageNo);
            writeFully(src, offset);
        }
    }

    /**
     * 按策略扩展一次：持 Lifecycle(S)+FileSize(X) → 计算增量 → 对 [oldSize, oldSize+inc) 写零页 → volatile 发布新 size。
     * 不 force（崩溃持久化 / WAL ordering 留后续 redo 切片）。
     *
     * <p>为什么只持 Lifecycle(S) 而非 X——扩展与普通读写可以安全并发：
     * (1) 新页在 currentSizeInPages 发布前对读不可见：读路径先取 volatile size 快照做越界检查，要么看到旧 size 而对
     *     新页返回 PageOutOfBoundsException，要么看到新 size，而此时零填充已完成（volatile 写与后续 volatile 读
     *     建立 happens-before），不会读到半初始化字节。
     * (2) 对已有页的并发写只落在 [0, oldSize)，与本方法零填充的 [oldSize, oldSize+inc) 区间不重叠。
     * 故 S 足以保证并发 IO 安全，X 只留给 drop/truncate/close；FileSize(X) 仅用于串行化并发扩展。
     *
     * <p>部分失败语义：若 zeroFill 中途抛 IOException，磁盘文件可能已部分增长，但 currentSizeInPages 仍停留在
     * oldSize（未发布）。currentSizeInPages 是权威逻辑大小，读路径据此拒绝未发布尾部，不会读到半初始化页；
     * 后续重试 autoExtend 会从 oldSize 重新零填充（重复写零无害）。磁盘物理大小与逻辑大小的暂时背离由未来
     * redo/recovery 切片统一收敛。
     *
     * @param policy 扩展策略。
     * @return 扩展后的 currentSizeInPages。
     */
    long autoExtend(AutoExtendPolicy policy) {
        if (policy == null) {
            throw new DatabaseValidationException("auto extend policy must not be null");
        }
        try (ResourceGuard s = lifecycleLatch.acquireShared(); ResourceGuard x = fileSizeLock.acquire()) {
            ensureOpen();
            long oldSize = currentSizeInPages;
            long inc = policy.nextIncrementPages(oldSize, pageSize);
            if (inc < 1) {
                throw new DatabaseValidationException("auto extend increment must be >= 1: " + inc);
            }
            zeroFill(channel, oldSize, oldSize + inc, pageSize.bytes());
            currentSizeInPages = oldSize + inc;
            return currentSizeInPages;
        }
    }

    /**
     * 当前物理大小页数（越界检查与上层分配的物理依据）。
     *
     * <p>返回的是 volatile 快照，返回后可能被并发 autoExtend 增大；仅用作物理依据，不保证调用方的
     * read-your-own-write 一致性。需要稳定快照时应在 FileSize(X) 下读取。
     *
     * @return currentSizeInPages 的 volatile 快照。
     */
    long currentSizeInPages() {
        return currentSizeInPages;
    }

    /**
     * 对数据文件执行 fsync/force。持 Lifecycle(S) 表示与普通 page IO 同级；close/drop/truncate 需要 Lifecycle(X)，
     * 会等待本次 force 离开。force 不回调 Buffer Pool 或 redo，避免物理文件锁反向进入上层等待。
     */
    void force() {
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            ensureOpen();
            try {
                channel.force(true);
            } catch (IOException e) {
                throw new DataFilePhysicalException("force data file failed: " + path, e);
            }
        }
    }

    /**
     * 关闭文件。持 Lifecycle(X)（获取即 drain 所有 S 持有者）→ 置 closed → 关 channel。
     */
    @Override
    public void close() {
        try (ResourceGuard ignored = lifecycleLatch.acquireExclusive()) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException e) {
                throw new DataFilePhysicalException("close data file failed: " + path, e);
            }
        }
    }

    private long boundedOffset(PageNo pageNo) {
        if (pageNo == null) {
            throw new DatabaseValidationException("page no must not be null");
        }
        long size = currentSizeInPages;
        if (pageNo.value() >= size) {
            throw new PageOutOfBoundsException("page out of bounds: space=" + spaceId.value()
                    + " pageNo=" + pageNo.value() + " size=" + size);
        }
        return PageId.of(spaceId, pageNo).offset(pageSize);
    }

    private void ensureOpen() {
        // 调用方已持 Lifecycle(S)；close() 必须先拿 Lifecycle(X) 才能置 closed，而 X 会等所有 S 离开（drain），
        // 故通过本检查后 close 无法在本次 IO 完成前生效——此处 volatile 读不存在有害 TOCTOU。
        if (closed) {
            throw new TablespaceNotOpenException("data file handle closed: space=" + spaceId.value());
        }
    }

    private void requirePageSized(ByteBuffer buffer) {
        if (buffer == null) {
            throw new DatabaseValidationException("page buffer must not be null");
        }
        if (buffer.remaining() != pageSize.bytes()) {
            throw new DatabaseValidationException("page buffer remaining must equal page size: expected "
                    + pageSize.bytes() + " got " + buffer.remaining());
        }
    }

    private void readFully(ByteBuffer dst, long offset) {
        long pos = offset;
        try {
            while (dst.hasRemaining()) {
                int n = channel.read(dst, pos);
                if (n < 0) {
                    throw new DataFilePhysicalException("unexpected EOF reading page at offset " + offset + " of " + path);
                }
                pos += n;
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("read page failed at offset " + offset + " of " + path, e);
        }
    }

    private void writeFully(ByteBuffer src, long offset) {
        long pos = offset;
        try {
            while (src.hasRemaining()) {
                pos += channel.write(src, pos);
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("write page failed at offset " + offset + " of " + path, e);
        }
    }

    private static void zeroFill(FileChannel channel, long fromPage, long toPage, int pageBytes) {
        ByteBuffer zero = ByteBuffer.allocate(pageBytes);
        try {
            for (long page = fromPage; page < toPage; page++) {
                zero.clear();
                long pos = Math.multiplyExact(page, (long) pageBytes);
                while (zero.hasRemaining()) {
                    pos += channel.write(zero, pos);
                }
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("zero-fill failed [" + fromPage + "," + toPage + ")", e);
        }
    }

    private static void validate(SpaceId spaceId, Path path, PageSize pageSize) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (path == null) {
            throw new DatabaseValidationException("data file path must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // 创建/打开失败的清理路径，原始异常更重要，关闭失败忽略。
            }
        }
    }
}
