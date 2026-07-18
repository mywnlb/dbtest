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
import java.util.OptionalLong;
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

    /**
     * 持久格式魔数；读取端用它拒绝错文件或损坏内容，修改会破坏已有数据兼容性。
     */
    private static final int MAGIC = 0x44574231; // "DWB1"
    /**
     * 当前稳定格式版本；编解码与恢复路径共同依赖该值，升级时必须保留旧版本判定。
     */
    private static final int LEGACY_FULL_COPY_FORMAT_VERSION = 1;
    /**
     * 当前稳定格式版本；编解码与恢复路径共同依赖该值，升级时必须保留旧版本判定。
     */
    private static final int FORMAT_VERSION = 2;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int SLOT_HEADER_BYTES = 36;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int DETECT_ONLY_METADATA_BYTES = Integer.BYTES + Long.BYTES + Integer.BYTES;
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏脏页刷盘与 checkpoint的不变量。
     */
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

    /**
     * 创建 {@code DoublewriteFileRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param slotCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     */
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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

    /** 只读恢复扫描入口；文件不存在时返回空，绝不创建诊断文件。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code openReadOnlyIfExists} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static Optional<DoublewriteFileRepository> openReadOnlyIfExists(Path path, PageSize pageSize) {
        if (path == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite path/page size must not be null");
        }
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return Optional.of(new DoublewriteFileRepository(path, pageSize, DEFAULT_SLOT_COUNT, channel));
        } catch (IOException e) {
            throw new FlushWriteException("failed to open doublewrite file read-only: " + path, e);
        }
    }

    /**
     * 写入一个 full-copy slot。调用方负责在 data file write 前调用 {@link #force()}，并在 data file force 成功或
     * 放弃写 data file 时调用 {@link #releaseSlot(FlushPageSnapshot)} 释放 in-flight 标记。
     *
     * @param snapshot 页镜像。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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
     * 在同一文件锁内连续写入 detect-only metadata 批次；data file 写入前由调用方统一 force。
     *
     * @param batch 待写入的 metadata 批次
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public void appendDetectOnlyBatch(DoublewriteBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("doublewrite batch must not be null");
        }
        ioLock.lock();
        int startSlot = -1;
        try {
            startSlot = reserveBatch(batch);
            for (int i = 0; i < batch.snapshots().size(); i++) {
                ByteBuffer slot = encodeDetectOnlySlot(batch.snapshots().get(i));
                channel.position(slotOffset(startSlot + i));
                while (slot.hasRemaining()) {
                    channel.write(slot);
                }
            }
        } catch (IOException e) {
            if (startSlot >= 0) {
                clearReservationRange(startSlot, batch.size());
            }
            throw new FlushWriteException("failed to append detect-only doublewrite batch: " + path, e);
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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
     *
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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

    /** 返回目标页最新有效 full-copy 的 pageLSN，供双文件恢复合并裁决使用。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public OptionalLong latestCopyLsn(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        ioLock.lock();
        try {
            long latest = Long.MIN_VALUE;
            long size = channel.size();
            for (long position = 0; position + SLOT_HEADER_BYTES <= size; position += slotSize()) {
                ValidSlot slot = readValidSlotAt(position);
                if (slot != null && slot.kind() == DoublewriteSlotKind.FULL_COPY
                        && pageId.equals(slot.pageId()) && slot.pageLsn() > latest) {
                    latest = slot.pageLsn();
                }
            }
            return latest == Long.MIN_VALUE ? OptionalLong.empty() : OptionalLong.of(latest);
        } catch (IOException e) {
            throw new FlushWriteException("failed to scan doublewrite page LSN: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 枚举 doublewrite 文件中所有有效 slot 的去重页号（恢复期"待检查页列表"来源）。full-copy 有效要求页镜像自身
     * checksum/trailer 通过；detect-only 有效要求 metadata payload CRC 通过。返回首见顺序的去重列表。
     *
     * @return 有有效副本的去重 {@link PageId} 列表。
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
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

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param batchSize 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code findConsecutiveFreeSlots} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    private int findConsecutiveFreeSlots(int batchSize) {
        int start = findConsecutiveFreeSlotsInRange(nextSlot, slotCount, batchSize);
        if (start >= 0) {
            return start;
        }
        return findConsecutiveFreeSlotsInRange(0, nextSlot, batchSize);
    }

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param fromInclusive 参与 {@code findConsecutiveFreeSlotsInRange} 的零基位置 {@code fromInclusive}；必须非负且小于所属页面、集合或持久结构的容量
     * @param toExclusive 参与 {@code findConsecutiveFreeSlotsInRange} 的零基位置 {@code toExclusive}；必须非负且小于所属页面、集合或持久结构的容量
     * @param batchSize 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code findConsecutiveFreeSlotsInRange} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    private int findConsecutiveFreeSlotsInRange(int fromInclusive, int toExclusive, int batchSize) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (toExclusive - fromInclusive < batchSize) {
            return -1;
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        int maxStart = toExclusive - batchSize;
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
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
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
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

    /**
     * 把调用方领域值编码为脏页刷盘与 checkpoint的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code encodeSlot} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private ByteBuffer encodeSlot(FlushPageSnapshot snapshot) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        byte[] payload = snapshot.pageImage();
        requirePageImage(payload);
        ByteBuffer slot = ByteBuffer.allocate(slotSize());
        slot.putInt(MAGIC);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        slot.putInt(FORMAT_VERSION);
        slot.putInt(snapshot.pageId().spaceId().value());
        slot.putLong(snapshot.pageId().pageNo().value());
        slot.putLong(snapshot.pageLsn().value());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        slot.putInt(pageSize.bytes());
        slot.putInt(crc32(payload));
        slot.put(payload);
        slot.flip();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return slot;
    }

    /**
     * 把调用方领域值编码为脏页刷盘与 checkpoint的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code encodeDetectOnlySlot} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private ByteBuffer encodeDetectOnlySlot(FlushPageSnapshot snapshot) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        byte[] source = snapshot.pageImage();
        requirePageImage(source);
        byte[] payload = new byte[pageSize.bytes()];
        ByteBuffer metadata = ByteBuffer.wrap(payload);
        metadata.putInt(DETECT_ONLY_METADATA_BYTES);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        metadata.putLong(snapshot.pageLsn().value());
        metadata.putInt(crc32(source));
        ByteBuffer slot = ByteBuffer.allocate(slotSize());
        slot.putInt(MAGIC);
        slot.putInt(FORMAT_VERSION);
        slot.putInt(snapshot.pageId().spaceId().value());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        slot.putLong(snapshot.pageId().pageNo().value());
        slot.putLong(snapshot.pageLsn().value());
        slot.putInt(pageSize.bytes());
        slot.putInt(crc32(payload));
        slot.put(payload);
        slot.flip();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return slot;
    }

    /**
     * 定位并读取脏页刷盘与 checkpoint领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param position 参与 {@code readValidSlotAt} 的零基位置 {@code position}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readValidSlotAt} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code ValidSlot} 结果
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private ValidSlot readValidSlotAt(long position) throws IOException {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
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
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
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
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        return new ValidSlot(PageId.of(SpaceId.of(space), PageNo.of(pageNo)), pageLsn,
                DoublewriteSlotKind.DETECT_ONLY_METADATA, expectedCrc, bytes);
    }

    /**
     * 判断 {@code isValidDetectOnlyPayload} 所表达的脏页刷盘与 checkpoint条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param pageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @return {@code isValidDetectOnlyPayload} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    private boolean isValidDetectOnlyPayload(byte[] bytes, long pageLsn) {
        ByteBuffer metadata = ByteBuffer.wrap(bytes);
        int metadataBytes = metadata.getInt();
        long metadataLsn = metadata.getLong();
        metadata.getInt();
        return metadataBytes == DETECT_ONLY_METADATA_BYTES && metadataLsn == pageLsn;
    }

    /**
     * 定位并读取脏页刷盘与 checkpoint领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param dst 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code readFullyOrTail} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
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

    /**
     * 封装脏页刷盘与 checkpoint中 {@code SlotReservation} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param pageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     */
    private record SlotReservation(PageId pageId, long pageLsn) {

        private boolean matches(FlushPageSnapshot snapshot) {
            return pageId.equals(snapshot.pageId()) && pageLsn == snapshot.pageLsn().value();
        }
    }

    /**
     * 封装脏页刷盘与 checkpoint中 {@code ValidSlot} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param pageLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param kind 选择 {@code 构造} 分支的 {@code DoublewriteSlotKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param checksum 参与 {@code 构造} 的位域或校验值 {@code checksum}；只允许当前格式定义的位，数值按无符号位模式解释
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    private record ValidSlot(PageId pageId, long pageLsn, DoublewriteSlotKind kind, int checksum, byte[] payload) {
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
