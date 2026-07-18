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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 固定文件环 LogBlock v1 repository。每个 MTR batch 在写前完整编码并检查单文件容量；放不下时整批轮转，
 * 永不把一个 batch chain 拆到两个文件。checkpoint 只推进可回收 LSN，真正 reset 仍发生在后续轮转。
 *
 * <p>恢复先校验完整 ring 文件集合与 v2 header，再按 startLsn 排序。只有逻辑最后文件允许 torn tail；较早文件
 * 的短块、checksum 失败或未闭合 chain 意味着中段缺口，必须 fail-closed。
 *
 * <p>单把 {@link #ioLock} 串行 append/force/scan/rotate；锁内不获取上层 page/trx 锁。READ_ONLY_VALIDATE
 * 以只读 channel 打开，并拒绝 append、force 和回收边界变更。
 */
public final class RotatingRedoLogRepository implements RedoLogFileRepository, RedoReclaimBoundary {

    /** ring 文件头 magic：ASCII "RLFR"。 */
    static final int HEADER_MAGIC = 0x524C4652;
    /** v2 表示文件内容由固定 LogBlock 构成；v1 是已拒绝的裸 batch-frame 格式。 */
    static final int FILE_FORMAT_VERSION = 2;
    /** header CRC 前缀：magic/version/fileId/inUse/startLsn。 */
    static final int HEADER_PREFIX_BYTES = 24;
    /** header 总长：前缀 + crc32。 */
    static final int FILE_HEADER_BYTES = 28;
    /** ring 文件名稳定格式。 */
    private static final Pattern FILE_NAME = Pattern.compile("redo-(\\d{6})\\.log");

    /** redo 目录，用于文件集合与异常诊断。 */
    private final Path dir;
    /** 单文件 LogBlock 区容量，不含 header，且按 512B 对齐。 */
    private final int maxBlockBytesPerFile;
    /** 环中文件；数组下标、header fileId 与文件名编号必须一致。 */
    private final RingFile[] files;
    /** 是否允许物理/内存写状态变化。 */
    private final boolean writable;
    /** 串行所有 channel 和 ring 状态。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 当前追加文件下标，由最后 startLsn 最高的 in-use 文件恢复。 */
    private int activeIndex;
    /** 后续 batch chain 首 blockNo；由全部保留文件连续扫描恢复。 */
    private long nextBlockNo;
    /** 后续 batch 必须使用的逻辑 start LSN。 */
    private long endLsn;
    /** 最早 in-use 文件 header 声明的 LSN；即使只剩 torn chain 也作为 recovery 覆盖边界。 */
    private long retainedStartLsn;
    /** 已持久 checkpoint 允许回收的最高逻辑 LSN，只能单调前进。 */
    private long reclaimBoundary;

    /**
     * 创建 {@code RotatingRedoLogRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param maxBlockBytesPerFile 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param files 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param writable 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
     */
    private RotatingRedoLogRepository(Path dir, int maxBlockBytesPerFile,
                                      RingFile[] files, boolean writable) {
        this.dir = dir;
        this.maxBlockBytesPerFile = maxBlockBytesPerFile;
        this.files = files;
        this.writable = writable;
        rebuildState();
    }

    /** 打开或创建 writable ring；目录无任何匹配文件时初始化 fresh v2 文件集合。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param fileCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param fileBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code open} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RotatingRedoLogRepository open(Path dir, int fileCount, long fileBytes) {
        return openInternal(dir, fileCount, fileBytes, true);
    }

    /** 只读打开完整的 existing ring；目录或任一预期文件缺失都会失败且不创建。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param fileCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param fileBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code openReadOnly} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RotatingRedoLogRepository openReadOnly(Path dir, int fileCount, long fileBytes) {
        return openInternal(dir, fileCount, fileBytes, false);
    }

    /**
     * 只探测目录中是否存在稳定命名的 ring 文件，不创建目录，也不把无关诊断文件视为 existing 实例。
     * StorageEngine 用该事实区分 fresh/existing；只要发现任意匹配文件就返回 true，完整性仍由 open 校验，
     * 因而 partial ring 不会被当作 fresh 后覆盖重建。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code hasAnyRingFiles} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static boolean hasAnyRingFiles(Path dir) {
        if (dir == null) {
            throw new DatabaseValidationException("redo ring dir must not be null");
        }
        try {
            return !discoverRingFiles(dir).isEmpty();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to inspect redo ring directory: " + dir, e);
        }
    }

    /**
     * 根据调用参数创建或转换 {@code openInternal} 返回的 {@code RotatingRedoLogRepository}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param fileCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param fileBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param writable 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
     * @return {@code openInternal} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private static RotatingRedoLogRepository openInternal(
            Path dir, int fileCount, long fileBytes, boolean writable) {
        validateOpenInputs(dir, fileCount, fileBytes);
        List<RingFile> opened = new ArrayList<>();
        try {
            Map<Integer, Path> existing = discoverRingFiles(dir);
            boolean fresh = existing.isEmpty();
            if (fresh && !writable) {
                throw new RedoLogIoException("read-only redo ring does not exist: " + dir);
            }
            if (fresh) {
                Files.createDirectories(dir);
            } else {
                validateCompleteFileSet(existing, fileCount, dir);
            }
            RingFile[] files = new RingFile[fileCount];
            for (int fileId = 0; fileId < fileCount; fileId++) {
                RingFile file = fresh
                        ? RingFile.createFresh(dir, fileId)
                        : RingFile.openExisting(existing.get(fileId), fileId, writable);
                files[fileId] = file;
                opened.add(file);
            }
            return new RotatingRedoLogRepository(dir, (int) fileBytes, files, writable);
        } catch (IOException e) {
            closeAfterOpenFailure(opened, e);
            throw new RedoLogIoException("failed to open redo ring: " + dir, e);
        } catch (RuntimeException e) {
            closeAfterOpenFailure(opened, e);
            throw e;
        }
    }

    /**
     * 追加完整 batch chain。编码/容量/LSN 校验在文件 mutation 前完成；若 active 尾部有 torn bytes，
     * {@link RingFile#appendBlocks} 才从最后可信 block 边界截断并覆盖。
     * @param batch redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void append(RedoLogBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        requireWritable("append");
        ioLock.lock();
        try {
            if (batch.range().start().value() != endLsn) {
                throw new RedoLogCorruptedException("redo ring append LSN is discontinuous: expected="
                        + endLsn + ", actual=" + batch.range().start().value());
            }
            RedoLogBlockCodec.EncodedBatch encoded = RedoLogBlockCodec.encodeBatch(batch, nextBlockNo);
            if (encoded.byteLength() > maxBlockBytesPerFile) {
                throw new DatabaseValidationException("redo batch LogBlock chain (" + encoded.byteLength()
                        + " bytes) exceeds redo ring file capacity (" + maxBlockBytesPerFile + " bytes)");
            }
            RingFile active = files[activeIndex];
            if (active.inUse && active.blockBytesUsed + encoded.byteLength() > maxBlockBytesPerFile) {
                rotate();
                active = files[activeIndex];
            }
            if (!active.inUse) {
                active.begin(batch.range().start().value());
            }
            active.appendBlocks(encoded.bytes(), encoded.byteLength(), batch.range().end().value());
            nextBlockNo = encoded.nextBlockNo();
            endLsn = batch.range().end().value();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to append redo LogBlocks to ring: " + dir, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 轮转只复用未使用或 endLsn 已被 checkpoint 覆盖的文件；失败前不改变 activeIndex。 */
    private void rotate() throws IOException {
        int next = (activeIndex + 1) % files.length;
        RingFile candidate = files[next];
        if (candidate.inUse && candidate.endLsn > reclaimBoundary) {
            throw new RedoLogCapacityExceededException("redo ring full: file " + candidate.fileId
                    + " holds redo through LSN " + candidate.endLsn
                    + " beyond reclaim boundary " + reclaimBoundary);
        }
        candidate.reset();
        activeIndex = next;
    }

    /** force 所有 in-use 文件，确保跨文件 write boundary 一次持久。
     *
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void force() {
        requireWritable("force");
        ioLock.lock();
        try {
            for (RingFile file : files) {
                if (file.inUse) {
                    file.force();
                }
            }
        } catch (IOException e) {
            throw new RedoLogIoException("failed to force redo ring: " + dir, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 重新扫描所有保留文件并返回不可变 LSN 顺序批次；扫描不 truncate 或修复文件。 */
    @Override
    public List<RedoLogBatch> readBatches() {
        return readRecoveryScan().batches();
    }

    /** 在同一 ioLock 临界区返回 batches 与 ring header 边界，避免 torn-only 文件的非零起点丢失。
     *
     * @return {@code readRecoveryScan} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    @Override
    public RedoRecoveryScan readRecoveryScan() {
        ioLock.lock();
        try {
            List<RedoLogBatch> batches = rebuildState();
            return new RedoRecoveryScan(
                    batches, Lsn.of(retainedStartLsn), Lsn.of(endLsn));
        } finally {
            ioLock.unlock();
        }
    }

    /** 当前 repository 的 LogBlock data 格式版本。 */
    @Override
    public int formatVersion() {
        return RedoLogBlockCodec.FORMAT_VERSION;
    }

    /** 单调推进内存回收边界；只读诊断实例禁止改变后续复用决策。
     *
     * @param checkpointLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void advanceReclaimBoundary(Lsn checkpointLsn) {
        if (checkpointLsn == null) {
            throw new DatabaseValidationException("redo reclaim boundary must not be null");
        }
        requireWritable("advance reclaim boundary");
        ioLock.lock();
        try {
            if (checkpointLsn.value() > reclaimBoundary) {
                reclaimBoundary = checkpointLsn.value();
            }
        } finally {
            ioLock.unlock();
        }
    }

    /** 当前 active fileId，供容量/轮转诊断测试读取。
     *
     * @return {@code activeFileId} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    public int activeFileId() {
        ioLock.lock();
        try {
            return files[activeIndex].fileId;
        } finally {
            ioLock.unlock();
        }
    }

    /** 关闭全部 channel；多个关闭失败时保留首个 cause 并把其余加入 suppressed。
     *
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void close() {
        ioLock.lock();
        try {
            IOException first = null;
            for (RingFile file : files) {
                try {
                    file.closeFile();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
            if (first != null) {
                throw new RedoLogIoException("failed to close redo ring: " + dir, first);
            }
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 按 header startLsn 重建文件顺序、blockNo、endLsn 和 active 文件。非末文件不允许 torn；跨文件 LSN
     * 与 blockNo 必须连续。方法只更新内存快照，不改写任何 header/content。
     */
    private List<RedoLogBatch> rebuildState() {
        List<RingFile> used = new ArrayList<>();
        for (RingFile file : files) {
            if (file.inUse) {
                used.add(file);
            }
        }
        used.sort(Comparator.comparingLong(file -> file.startLsn));
        if (used.isEmpty()) {
            activeIndex = 0;
            nextBlockNo = 0;
            endLsn = 0;
            retainedStartLsn = 0;
            return List.of();
        }
        for (int i = 1; i < used.size(); i++) {
            if (used.get(i - 1).startLsn == used.get(i).startLsn) {
                throw new RedoLogCorruptedException(
                        "redo ring files have duplicate start LSN " + used.get(i).startLsn);
            }
        }

        List<RedoLogBatch> all = new ArrayList<>();
        OptionalLong expectedBlockNo = OptionalLong.empty();
        long expectedLsn = used.getFirst().startLsn;
        retainedStartLsn = expectedLsn;
        for (int i = 0; i < used.size(); i++) {
            RingFile file = used.get(i);
            if (file.startLsn != expectedLsn) {
                throw new RedoLogCorruptedException("redo ring file LSN gap: expected start="
                        + expectedLsn + ", file " + file.fileId + " starts=" + file.startLsn);
            }
            boolean logicalLast = i == used.size() - 1;
            RedoLogBlockScanResult result = file.scan(
                    logicalLast, expectedBlockNo, maxBlockBytesPerFile);
            if (!logicalLast && result.batches().isEmpty()) {
                throw new RedoLogCorruptedException(
                        "non-final redo ring file contains no complete batch: " + file.path);
            }
            all.addAll(result.batches());
            expectedLsn = result.endLsn().value();
            long recoveredNextBlockNo = result.nextBlockNo();
            if (expectedBlockNo.isEmpty() && result.batches().isEmpty() && result.tornTail()) {
                // 所有旧 block 证据都已被 checkpoint 回收，而 retained 首 chain 又 torn 时无法信任其 header blockNo。
                // 每个非空 batch 的逻辑字节数至少覆盖其物理 block 数，故 retained start LSN 一定不小于
                // 已回收 block 总数；以它作为跳号续写边界保持全局 blockNo 不回退，同时不猜测 torn header。
                recoveredNextBlockNo = Math.max(recoveredNextBlockNo, file.startLsn);
            }
            expectedBlockNo = OptionalLong.of(recoveredNextBlockNo);
        }
        activeIndex = used.getLast().fileId;
        nextBlockNo = expectedBlockNo.orElse(0L);
        endLsn = expectedLsn;
        return List.copyOf(all);
    }

    /**
     * 校验 {@code validateOpenInputs} 涉及的Redo/WAL结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param fileCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param fileBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static void validateOpenInputs(Path dir, int fileCount, long fileBytes) {
        if (dir == null) {
            throw new DatabaseValidationException("redo ring dir must not be null");
        }
        if (fileCount < 1) {
            throw new DatabaseValidationException("redo ring file count must be >= 1: " + fileCount);
        }
        if (fileBytes < RedoLogBlockCodec.BLOCK_BYTES || fileBytes > Integer.MAX_VALUE
                || fileBytes % RedoLogBlockCodec.BLOCK_BYTES != 0) {
            throw new DatabaseValidationException(
                    "redo ring fileBytes must be a 512-byte-aligned int: " + fileBytes);
        }
    }

    /** 枚举匹配稳定命名的文件；不相关诊断文件不参与 ring 完整性判断。 */
    private static Map<Integer, Path> discoverRingFiles(Path dir) throws IOException {
        Map<Integer, Path> files = new HashMap<>();
        if (!Files.exists(dir)) {
            return files;
        }
        if (!Files.isDirectory(dir)) {
            throw new RedoLogCorruptedException("redo ring path is not a directory: " + dir);
        }
        try (Stream<Path> stream = Files.list(dir)) {
            for (Path path : stream.toList()) {
                Matcher matcher = FILE_NAME.matcher(path.getFileName().toString());
                if (!matcher.matches()) {
                    continue;
                }
                int fileId = Integer.parseInt(matcher.group(1));
                if (files.put(fileId, path) != null) {
                    throw new RedoLogCorruptedException("duplicate redo ring fileId: " + fileId);
                }
            }
        }
        return files;
    }

    private static void validateCompleteFileSet(Map<Integer, Path> existing, int fileCount, Path dir) {
        if (existing.size() != fileCount) {
            throw new RedoLogCorruptedException("redo ring file set size mismatch in " + dir
                    + ": expected=" + fileCount + ", actual=" + existing.size());
        }
        for (int fileId = 0; fileId < fileCount; fileId++) {
            if (!existing.containsKey(fileId)) {
                throw new RedoLogCorruptedException("redo ring file set is missing fileId " + fileId);
            }
        }
    }

    /**
     * 释放本方法拥有的Redo/WAL资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param opened 参与 {@code closeAfterOpenFailure} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private static void closeAfterOpenFailure(List<RingFile> opened, Throwable original) {
        for (RingFile file : opened) {
            try {
                file.closeFile();
            } catch (IOException closeFailure) {
                original.addSuppressed(closeFailure);
            }
        }
    }

    private void requireWritable(String operation) {
        if (!writable) {
            throw new DatabaseValidationException(
                    "read-only redo ring cannot " + operation + ": " + dir);
        }
    }

    /** 单个 ring 文件；所有可变字段只在外层 ioLock 下访问。 */
    private static final class RingFile {

        /** 文件名/header/数组统一编号。 */
        private final int fileId;
        /** 物理路径，用于损坏诊断。 */
        private final Path path;
        /** 只读或读写 channel。 */
        private final FileChannel channel;
        /** 是否持有当前代 redo。 */
        private boolean inUse;
        /** 当前代首 batch start LSN。 */
        private long startLsn;
        /** 最后完整 batch end LSN。 */
        private long endLsn;
        /** 最后完整 batch 后的 block 字节数，不含 header。 */
        private int blockBytesUsed;

        private RingFile(int fileId, Path path, FileChannel channel) {
            this.fileId = fileId;
            this.path = path;
            this.channel = channel;
        }

        private static RingFile createFresh(Path dir, int fileId) throws IOException {
            Path path = path(dir, fileId);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            RingFile file = new RingFile(fileId, path, channel);
            try {
                file.writeHeader(false, 0L);
                channel.force(true);
                file.markFree();
                return file;
            } catch (RuntimeException | IOException failure) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        /**
         * 根据调用参数创建或转换 {@code openExisting} 返回的 {@code RingFile}；输入先完成领域校验，成功结果不为 {@code null}。
         *
         * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
         * @param fileId 参与 {@code openExisting} 的原始数值身份 {@code fileId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
         * @param writable 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
         * @return {@code openExisting} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         */
        private static RingFile openExisting(Path path, int fileId, boolean writable) throws IOException {
            FileChannel channel = writable
                    ? FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
                    : FileChannel.open(path, StandardOpenOption.READ);
            RingFile file = new RingFile(fileId, path, channel);
            try {
                file.readHeader();
                return file;
            } catch (RuntimeException | IOException failure) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        /** existing header 任何结构错误都 fail-closed，不再改写为空闲文件。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
         *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
         *     <li>执行核心状态转换与受控 IO，把必要的 redo、dirty 或诊断副作用交给既有下游。</li>
         *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
         * </ol>
         *
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
         * @throws RedoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        private void readHeader() throws IOException {
            // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
            if (channel.size() < FILE_HEADER_BYTES) {
                throw new RedoLogCorruptedException("redo ring header is truncated: " + path);
            }
            ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_BYTES);
            readFullyAt(0, header);
            header.flip();
            int magic = header.getInt();
            // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
            int version = header.getInt();
            int storedFileId = header.getInt();
            int inUseFlag = header.getInt();
            long storedStartLsn = header.getLong();
            int storedCrc = header.getInt();
            // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换与受控 IO，并维持领域不变量。
            if (magic != HEADER_MAGIC) {
                throw new RedoLogCorruptedException("redo ring header magic mismatch: " + path);
            }
            if (storedCrc != headerCrc(magic, version, storedFileId, inUseFlag, storedStartLsn)) {
                throw new RedoLogCorruptedException("redo ring header checksum is invalid: " + path);
            }
            if (version == 1) {
                throw new RedoLogFormatException("legacy redo ring header v1 is not supported: " + path);
            }
            if (version != FILE_FORMAT_VERSION) {
                throw new RedoLogFormatException("unsupported redo ring header version " + version + ": " + path);
            }
            if (storedFileId != fileId || (inUseFlag != 0 && inUseFlag != 1)) {
                throw new RedoLogCorruptedException("redo ring header fields are invalid: " + path);
            }
            // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
            if (inUseFlag == 0) {
                if (storedStartLsn != 0) {
                    throw new RedoLogCorruptedException("free redo ring file has non-zero start LSN: " + path);
                }
                markFree();
            } else {
                if (storedStartLsn < 0) {
                    throw new RedoLogCorruptedException("redo ring file has negative start LSN: " + path);
                }
                inUse = true;
                startLsn = storedStartLsn;
                endLsn = storedStartLsn;
                blockBytesUsed = 0;
            }
        }

        private RedoLogBlockScanResult scan(boolean allowTornTail, OptionalLong expectedBlockNo,
                                            int configuredBlockBytes) {
            if (!inUse) {
                return new RedoLogBlockScanResult(List.of(), 0,
                        expectedBlockNo.orElse(0L), Lsn.of(0), false);
            }
            try {
                long contentLength = channel.size() - FILE_HEADER_BYTES;
                if (contentLength < 0) {
                    throw new RedoLogCorruptedException("redo ring file was truncated below its header: " + path);
                }
                if (contentLength > configuredBlockBytes) {
                    throw new RedoLogCorruptedException("redo ring file content exceeds configured block capacity: "
                            + path + ", content=" + contentLength + ", configured=" + configuredBlockBytes);
                }
                ByteBuffer content = ByteBuffer.allocate((int) contentLength);
                readFullyAt(FILE_HEADER_BYTES, content);
                content.flip();
                RedoLogBlockScanResult result = RedoLogBlockScanner.scan(
                        content, allowTornTail, expectedBlockNo, startLsn);
                blockBytesUsed = result.validBytes();
                endLsn = result.endLsn().value();
                return result;
            } catch (IOException e) {
                throw new RedoLogIoException("failed to scan redo ring file: " + path, e);
            }
        }

        /** checkpoint 允许复用后清除内容并写空闲 v2 header；下次 begin 再声明新 startLsn。 */
        private void reset() throws IOException {
            channel.truncate(FILE_HEADER_BYTES);
            writeHeader(false, 0L);
            markFree();
        }

        private void begin(long startLsn) throws IOException {
            channel.truncate(FILE_HEADER_BYTES);
            writeHeader(true, startLsn);
            inUse = true;
            this.startLsn = startLsn;
            endLsn = startLsn;
            blockBytesUsed = 0;
        }

        /** 写入前截掉 scanner 已判定的 torn 后缀；成功返回后才发布 blockBytesUsed/endLsn。
         *
         * @param blocks 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
         * @param byteLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
         * @param batchEndLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         */
        private void appendBlocks(ByteBuffer blocks, int byteLength, long batchEndLsn) throws IOException {
            long writeOffset = FILE_HEADER_BYTES + (long) blockBytesUsed;
            if (channel.size() != writeOffset) {
                channel.truncate(writeOffset);
            }
            writeFullyAt(writeOffset, blocks);
            blockBytesUsed += byteLength;
            endLsn = batchEndLsn;
        }

        private void force() throws IOException {
            channel.force(true);
        }

        /**
         * 校验输入与当前状态后修改Redo/WAL领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
         *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
         *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
         *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
         * </ol>
         *
         * @param used 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
         * @param startLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         */
        private void writeHeader(boolean used, long startLsn) throws IOException {
            // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
            int flag = used ? 1 : 0;
            ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_BYTES);
            header.putInt(HEADER_MAGIC);
            // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
            header.putInt(FILE_FORMAT_VERSION);
            header.putInt(fileId);
            header.putInt(flag);
            // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
            header.putLong(startLsn);
            header.putInt(headerCrc(HEADER_MAGIC, FILE_FORMAT_VERSION, fileId, flag, startLsn));
            header.flip();
            // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
            writeFullyAt(0, header);
        }

        /**
         * 更新 {@code markFree} 指定的Redo/WAL局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
         */
        private void markFree() {
            inUse = false;
            startLsn = 0;
            endLsn = 0;
            blockBytesUsed = 0;
        }

        private void closeFile() throws IOException {
            channel.close();
        }

        /**
         * 定位并读取Redo/WAL领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
         *
         * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
         * @param destination 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
         */
        private void readFullyAt(long offset, ByteBuffer destination) throws IOException {
            long position = offset;
            while (destination.hasRemaining()) {
                int read = channel.read(destination, position);
                if (read < 0) {
                    throw new RedoLogIoException("unexpected EOF while reading redo ring file: " + path);
                }
                if (read == 0) {
                    throw new RedoLogIoException("zero-progress read while reading redo ring file: " + path);
                }
                position += read;
            }
        }

        /** positional write 必须持续前进；零进度按 IO 失败处理，避免持 ring ioLock 无限自旋。
         *
         * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
         * @param source 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
         */
        private void writeFullyAt(long offset, ByteBuffer source) throws IOException {
            long position = offset;
            while (source.hasRemaining()) {
                int written = channel.write(source, position);
                if (written <= 0) {
                    throw new RedoLogIoException("zero-progress write while updating redo ring file: " + path);
                }
                position += written;
            }
        }

        private static Path path(Path dir, int fileId) {
            return dir.resolve(String.format("redo-%06d.log", fileId));
        }
    }

    private static int headerCrc(int magic, int version, int fileId, int inUse, long startLsn) {
        ByteBuffer prefix = ByteBuffer.allocate(HEADER_PREFIX_BYTES);
        prefix.putInt(magic);
        prefix.putInt(version);
        prefix.putInt(fileId);
        prefix.putInt(inUse);
        prefix.putLong(startLsn);
        return RedoLogBlockCodec.checksum(prefix.array(), HEADER_PREFIX_BYTES);
    }
}
