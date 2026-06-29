package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redo 文件环仓储（0.18a）：把单 append-only 文件换成固定数量、可轮转和回收的 redo 文件环，
 * 让长跑下 redo 占用有界、checkpoint 推进后旧区间可复用。批次帧格式与单文件仓储共用 {@link RedoBatchFrameCodec}，
 * 保证两种实现写出的 redo 在 crash recovery 时可互读。
 *
 * <p><b>文件布局</b>：目录下 {@code redo-000000.log ... redo-(N-1).log}，每文件前 {@value #FILE_HEADER_BYTES} 字节为
 * 文件头 {@code magic + version + fileId + inUse + startLsn + crc}；其后是连续帧。文件头的 {@code startLsn} 让恢复期
 * 无需独立 control 文件即可把各文件按 LSN 排序、跨文件顺序扫描。
 *
 * <p><b>轮转</b>：active 文件放不下整批时轮转到环中下一个文件（单批不跨文件）。
 *
 * <p><b>回收</b>：轮转只能复用「从未使用」或「最高 LSN ≤ 回收边界」的文件；回收边界由上层（checkpoint）经
 * {@link #advanceReclaimBoundary} 注入，本仓储不反向依赖 Buffer Pool / flush，守住 redo→上层的单向依赖。
 * 若环中下一个文件仍持有未 checkpoint 的 redo，则 fail-closed 抛 {@link RedoLogCapacityExceededException}，
 * 绝不覆盖崩溃恢复仍需要的 redo。复用文件时先 {@code truncate} 到文件头，丢弃上一代帧，避免恢复扫描误读陈旧帧。
 *
 * <p><b>并发边界</b>：单把 {@code ioLock} 串行 append/force/scan/rotate，所有文件头与 active 指针切换都在锁内原子完成；
 * 不使用 {@code synchronized}，释放走 try/finally。
 *
 * <p><b>简化点（与 MySQL/InnoDB 差异，待后续）</b>：单批不跨文件（依赖文件容量 ≥ 单批上限）；无 log block
 * header/trailer/checksum（仍用 batch frame + CRC32，0.20）；无容量分级 throttle（环满直接 fail-closed，0.6）；
 * 文件非预分配，随 append 增长。
 */
public final class RotatingRedoLogRepository implements RedoLogFileRepository, RedoReclaimBoundary {

    /** 文件头 magic：ASCII "RLFR"。 */
    private static final int HEADER_MAGIC = 0x524C4652;
    /** 文件头格式版本。 */
    private static final int FORMAT_VERSION = 1;
    /** 文件头 CRC 覆盖范围：magic(4)+version(4)+fileId(4)+inUse(4)+startLsn(8) = 24。 */
    private static final int HEADER_PREFIX_BYTES = 24;
    /** 文件头总字节：前缀 24 + crc(4) = 28。 */
    private static final int FILE_HEADER_BYTES = HEADER_PREFIX_BYTES + 4;

    /** redo 目录，用于异常诊断。 */
    private final Path dir;
    /** 单个文件可容纳的帧字节上限（不含文件头）；超过该值的单批视为配置错误。 */
    private final int maxFrameBytesPerFile;
    /** 环中文件，下标即环位置；fileId 与下标一致。 */
    private final RingFile[] files;
    /** 串行 append/force/scan/rotate；所有共享状态读写都在该锁内。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 当前用于追加的文件下标。 */
    private int activeIndex;
    /**
     * 回收边界：最高 LSN ≤ 该值的文件可被复用。来自上层持久化的 checkpoint LSN；
     * 不变量：只能单调前进，且必须 ≤ 已持久 checkpoint，否则会覆盖恢复仍需要的 redo。
     */
    private long reclaimBoundary;

    private RotatingRedoLogRepository(Path dir, int maxFrameBytesPerFile, RingFile[] files, int activeIndex) {
        this.dir = dir;
        this.maxFrameBytesPerFile = maxFrameBytesPerFile;
        this.files = files;
        this.activeIndex = activeIndex;
        this.reclaimBoundary = 0L;
    }

    /**
     * 打开或创建一个 redo 文件环。已存在的文件读取文件头并扫描帧重建 inUse/startLsn/endLsn/写偏移（丢弃 torn tail）；
     * active 文件选为已用文件中 endLsn 最高者（续写点），全新环则为 0 号文件。回收边界复位为 0，由上层重新注入。
     *
     * @param dir                  redo 目录。
     * @param fileCount            文件数（≥1）。
     * @param maxFrameBytesPerFile 单文件帧容量上限（不含文件头，&gt;0）。
     * @return 已打开的文件环仓储。
     */
    public static RotatingRedoLogRepository open(Path dir, int fileCount, long maxFrameBytesPerFile) {
        if (dir == null) {
            throw new DatabaseValidationException("redo ring dir must not be null");
        }
        if (fileCount < 1) {
            throw new DatabaseValidationException("redo ring file count must be >= 1: " + fileCount);
        }
        if (maxFrameBytesPerFile <= 0 || maxFrameBytesPerFile > Integer.MAX_VALUE) {
            throw new DatabaseValidationException("redo ring file capacity out of range: " + maxFrameBytesPerFile);
        }
        try {
            Files.createDirectories(dir);
            RingFile[] files = new RingFile[fileCount];
            for (int i = 0; i < fileCount; i++) {
                files[i] = RingFile.openOrCreate(dir, i);
            }
            int active = 0;
            long maxEnd = Long.MIN_VALUE;
            for (int i = 0; i < fileCount; i++) {
                if (files[i].inUse && files[i].endLsn > maxEnd) {
                    maxEnd = files[i].endLsn;
                    active = i;
                }
            }
            return new RotatingRedoLogRepository(dir, (int) maxFrameBytesPerFile, files, active);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open redo ring dir: " + dir, e);
        }
    }

    /**
     * 追加一个完整 redo 批次。
     *
     * <p>数据流：编码帧→若超过单文件容量则判为配置错误抛 {@link DatabaseValidationException}→若 active 文件放不下整批则
     * {@link #rotate} 到下一个可回收文件（无可回收文件时 fail-closed，且不改变任何状态）→必要时初始化 active 文件头→
     * 写帧并推进该文件的 endLsn/写偏移。调用方须按 LSN 顺序调用（与 {@link RedoLogManager} 的串行 writer 一致）。
     *
     * @param batch 待写入批次。
     */
    @Override
    public void append(RedoLogBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        ByteBuffer frame = RedoBatchFrameCodec.encodeFrame(batch);
        int frameSize = frame.remaining();
        if (frameSize > maxFrameBytesPerFile) {
            throw new DatabaseValidationException("redo batch frame (" + frameSize
                    + " bytes) exceeds redo file capacity (" + maxFrameBytesPerFile + " bytes)");
        }
        long start = batch.range().start().value();
        long end = batch.range().end().value();
        ioLock.lock();
        try {
            RingFile active = files[activeIndex];
            if (active.inUse && active.frameBytesUsed + frameSize > maxFrameBytesPerFile) {
                // rotate 在改写状态前先校验下一个文件可回收；不可回收则抛异常，append 整体无副作用。
                rotate();
                active = files[activeIndex];
            }
            if (!active.inUse) {
                active.begin(start);
            }
            active.appendFrame(frame, end);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to append redo batch to ring: " + dir, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 轮转到环中下一个文件。只有当下一个文件「从未使用」或「最高 LSN ≤ 回收边界」时才允许复用；
     * 否则抛 {@link RedoLogCapacityExceededException}（此前不修改任何状态，保证失败的 append 不破坏已有 redo）。
     */
    private void rotate() throws IOException {
        int next = (activeIndex + 1) % files.length;
        RingFile candidate = files[next];
        boolean reclaimable = !candidate.inUse || candidate.endLsn <= reclaimBoundary;
        if (!reclaimable) {
            throw new RedoLogCapacityExceededException("redo ring full: file " + candidate.fileId
                    + " still holds un-checkpointed redo up to LSN " + candidate.endLsn
                    + " (reclaim boundary " + reclaimBoundary + "); advance checkpoint before appending");
        }
        candidate.reset();
        activeIndex = next;
    }

    /** 对所有持有 redo 的文件执行 fsync。轮转可能跨文件，故 force 覆盖全部 inUse 文件以保证已写 LSN 落盘。 */
    @Override
    public void force() {
        ioLock.lock();
        try {
            for (RingFile f : files) {
                if (f.inUse) {
                    f.force();
                }
            }
        } catch (IOException e) {
            throw new RedoLogIoException("failed to force redo ring: " + dir, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 跨文件按 LSN 顺序读出当前保留的全部完整批次。inUse 文件按 {@code startLsn} 升序拼接，单文件内遇 torn tail 停止。
     * 已回收并复用的文件只返回新一代帧；尚未复用的旧文件其旧帧由调用方（{@link RedoRecoveryReader}）按 checkpoint 过滤。
     *
     * @return 按 LSN 顺序排列的完整批次。
     */
    @Override
    public List<RedoLogBatch> readBatches() {
        ioLock.lock();
        try {
            List<RingFile> used = new ArrayList<>();
            for (RingFile f : files) {
                if (f.inUse) {
                    used.add(f);
                }
            }
            used.sort(Comparator.comparingLong(f -> f.startLsn));
            List<RedoLogBatch> out = new ArrayList<>();
            for (RingFile f : used) {
                out.addAll(f.readBatches());
            }
            return out;
        } catch (IOException e) {
            throw new RedoLogIoException("failed to read redo ring: " + dir, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 推进回收边界（单调）。调用方必须传入已持久化的 checkpoint LSN：低于该值的 redo 已被 checkpoint 覆盖、
     * 不再为崩溃恢复所需，对应文件方可在轮转时复用。
     *
     * @param checkpointLsn 已持久 checkpoint LSN。
     */
    @Override
    public void advanceReclaimBoundary(Lsn checkpointLsn) {
        if (checkpointLsn == null) {
            throw new DatabaseValidationException("redo reclaim boundary must not be null");
        }
        ioLock.lock();
        try {
            if (checkpointLsn.value() > reclaimBoundary) {
                reclaimBoundary = checkpointLsn.value();
            }
        } finally {
            ioLock.unlock();
        }
    }

    /** 当前 active 文件的 fileId（诊断 / 测试观测轮转用）。 */
    public int activeFileId() {
        ioLock.lock();
        try {
            return files[activeIndex].fileId;
        } finally {
            ioLock.unlock();
        }
    }

    @Override
    public void close() {
        ioLock.lock();
        try {
            for (RingFile f : files) {
                f.closeFile();
            }
        } catch (IOException e) {
            throw new RedoLogIoException("failed to close redo ring: " + dir, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 单个环文件：持有 channel，并在内存中维护 inUse/startLsn/endLsn/写偏移。所有方法都在外层 {@code ioLock} 下调用，
     * 自身不再加锁。
     */
    private static final class RingFile {

        /** 文件下标兼 fileId。 */
        private final int fileId;
        /** 文件路径，用于诊断。 */
        private final Path path;
        /** 文件 channel。 */
        private final FileChannel channel;
        /** 是否持有当代 redo。false 表示空闲/已回收，可被复用。 */
        private boolean inUse;
        /** 当代第一条批次的起始 LSN，恢复期据此跨文件排序。 */
        private long startLsn;
        /** 当代最后一条批次的结束 LSN，用于回收判定（≤ 回收边界即可复用）。 */
        private long endLsn;
        /** 已写入帧的字节数（不含文件头），决定下一帧写偏移与剩余容量。 */
        private int frameBytesUsed;

        private RingFile(int fileId, Path path, FileChannel channel) {
            this.fileId = fileId;
            this.path = path;
            this.channel = channel;
        }

        /** 打开或创建文件，并加载文件头 + 扫描帧重建内存状态。 */
        private static RingFile openOrCreate(Path dir, int fileId) throws IOException {
            Path path = dir.resolve(String.format("redo-%06d.log", fileId));
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            RingFile file = new RingFile(fileId, path, channel);
            file.loadHeaderAndScan();
            return file;
        }

        /**
         * 重建内存状态：文件头缺失/损坏当作空闲并写回空闲头；inUse 文件扫描帧得到 endLsn 与写偏移，
         * decodeFrames 在 torn tail 处停止，故写偏移自动落在最后一条完整帧之后，下次 append 覆盖 torn tail。
         */
        private void loadHeaderAndScan() throws IOException {
            if (channel.size() < FILE_HEADER_BYTES) {
                writeHeader(false, 0L);
                markFree();
                return;
            }
            ByteBuffer hb = ByteBuffer.allocate(FILE_HEADER_BYTES);
            readFullyAt(0, hb);
            hb.flip();
            int magic = hb.getInt();
            int version = hb.getInt();
            int fid = hb.getInt();
            int inUseFlag = hb.getInt();
            long start = hb.getLong();
            int crc = hb.getInt();
            if (magic != HEADER_MAGIC || version != FORMAT_VERSION
                    || crc != headerCrc(magic, version, fid, inUseFlag, start)) {
                writeHeader(false, 0L);
                markFree();
                return;
            }
            if (inUseFlag == 0) {
                markFree();
                return;
            }
            inUse = true;
            startLsn = start;
            int contentLen = (int) (channel.size() - FILE_HEADER_BYTES);
            List<RedoLogBatch> batches = List.of();
            if (contentLen > 0) {
                ByteBuffer content = ByteBuffer.allocate(contentLen);
                readFullyAt(FILE_HEADER_BYTES, content);
                content.flip();
                batches = RedoBatchFrameCodec.decodeFrames(content);
                frameBytesUsed = content.position();
            } else {
                frameBytesUsed = 0;
            }
            endLsn = batches.isEmpty() ? start : batches.get(batches.size() - 1).range().end().value();
        }

        private void markFree() {
            inUse = false;
            startLsn = 0;
            endLsn = 0;
            frameBytesUsed = 0;
        }

        /** 复用前重置：truncate 丢弃上一代帧并写回空闲头，避免恢复扫描误读陈旧帧。 */
        private void reset() throws IOException {
            channel.truncate(FILE_HEADER_BYTES);
            writeHeader(false, 0L);
            markFree();
        }

        /** 把空闲文件初始化为新一代 active：写 inUse=1 + startLsn 头，归零写偏移。 */
        private void begin(long start) throws IOException {
            writeHeader(true, start);
            inUse = true;
            startLsn = start;
            endLsn = start;
            frameBytesUsed = 0;
        }

        private void appendFrame(ByteBuffer frame, long batchEnd) throws IOException {
            int size = frame.remaining();
            channel.position(FILE_HEADER_BYTES + frameBytesUsed);
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
            frameBytesUsed += size;
            endLsn = batchEnd;
        }

        private void force() throws IOException {
            channel.force(true);
        }

        private List<RedoLogBatch> readBatches() throws IOException {
            if (!inUse) {
                return List.of();
            }
            int contentLen = (int) (channel.size() - FILE_HEADER_BYTES);
            if (contentLen <= 0) {
                return List.of();
            }
            ByteBuffer content = ByteBuffer.allocate(contentLen);
            readFullyAt(FILE_HEADER_BYTES, content);
            content.flip();
            return RedoBatchFrameCodec.decodeFrames(content);
        }

        private void writeHeader(boolean used, long start) throws IOException {
            int inUseFlag = used ? 1 : 0;
            ByteBuffer hb = ByteBuffer.allocate(FILE_HEADER_BYTES);
            hb.putInt(HEADER_MAGIC);
            hb.putInt(FORMAT_VERSION);
            hb.putInt(fileId);
            hb.putInt(inUseFlag);
            hb.putLong(start);
            hb.putInt(headerCrc(HEADER_MAGIC, FORMAT_VERSION, fileId, inUseFlag, start));
            hb.flip();
            channel.position(0);
            while (hb.hasRemaining()) {
                channel.write(hb);
            }
        }

        private void closeFile() throws IOException {
            channel.close();
        }

        private void readFullyAt(long position, ByteBuffer dst) throws IOException {
            long pos = position;
            while (dst.hasRemaining()) {
                int n = channel.read(dst, pos);
                if (n < 0) {
                    break;
                }
                pos += n;
            }
        }

        private static int headerCrc(int magic, int version, int fileId, int inUseFlag, long startLsn) {
            ByteBuffer b = ByteBuffer.allocate(HEADER_PREFIX_BYTES);
            b.putInt(magic);
            b.putInt(version);
            b.putInt(fileId);
            b.putInt(inUseFlag);
            b.putLong(startLsn);
            return RedoBatchFrameCodec.crc32(b.array());
        }
    }
}
