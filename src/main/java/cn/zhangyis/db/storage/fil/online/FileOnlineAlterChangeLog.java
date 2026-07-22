package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterChangeLog;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineChangeLogSnapshot;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlAbortReason;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * `OALTLOG1`通用Online ALTER journal。每个实例独占一个FileChannel，公平显式锁保护append顺序、
 * force high-water、状态机和关闭边界；candidate永远不能占用terminal reserve。
 *
 * <p>该实现故意不复用旧单索引文件格式：旧日志继续由`FileOnlineIndexChangeLog`恢复，通用协议可独立加入
 * READY_TO_PUBLISH、manifest及shadow identity而不改变历史字节语义。</p>
 */
public final class FileOnlineAlterChangeLog implements OnlineAlterChangeLog {

    /** 无状态header/frame codec。 */
    private final OnlineAlterRowLogCodec codec = new OnlineAlterRowLogCodec();
    /** 由受控online-ddl目录派生的规范绝对路径。 */
    private final Path path;
    /** 本实例独占的读写文件句柄。 */
    private final FileChannel channel;
    /** offset 0不可变owner与opaque manifest。 */
    private final OnlineAlterLogHeader header;
    /** reset时必须保留的精确header长度。 */
    private final long headerBytes;
    /** 文件容量硬上限，防止Online DDL吃尽磁盘。 */
    private final long maxBytes;
    /** candidate不可使用的终止证据预留。 */
    private final int terminalReserveBytes;
    /** 保护以下frame投影、sequence、phase、I/O位置和生命周期。 */
    private final ReentrantLock stateLock = new ReentrantLock(true);
    /** 当前generation全部已验证frame的有序投影。 */
    private final List<OnlineAlterLogRecord> records = new ArrayList<>();

    /** reset后单调递增的capture generation。 */
    private long generation = 1;
    /** 当前generation完整candidate数量。 */
    private long candidateCount;
    /** header与全部完整frame的已知文件长度。 */
    private long currentSizeBytes;
    /** 最后完整append的严格连续sequence。 */
    private long highestAppended;
    /** 最近成功force覆盖的sequence。 */
    private long highestForced;
    /** 当前generation是否已有唯一首frame。 */
    private boolean generationStarted;
    /** DML是否可追加candidate。 */
    private boolean capturing;
    /** candidate区间是否已封闭。 */
    private boolean sealed;
    /** 是否已持久形成可执行最终发布的准备证据。 */
    private boolean readyToPublish;
    /** 是否已形成禁止反向清理的reconciliation证据。 */
    private boolean reconciled;
    /** 存储层是否已经要求coordinator终止。 */
    private boolean abortRequired;
    /** I/O结果未知后进入fail-stop，禁止继续追加。 */
    private boolean failed;
    /** channel是否已关闭；关闭不删除持久证据。 */
    private boolean closed;

    private FileOnlineAlterChangeLog(Path path, FileChannel channel,
                                     OnlineAlterLogHeader header, long headerBytes,
                                     long maxBytes, int terminalReserveBytes) {
        this.path = path;
        this.channel = channel;
        this.header = header;
        this.headerBytes = headerBytes;
        this.maxBytes = maxBytes;
        this.terminalReserveBytes = terminalReserveBytes;
        this.currentSizeBytes = headerBytes;
    }

    /**
     * 创建新journal并先force immutable header，确保随后marker不会引用未持久manifest。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证容量与精确新路径，拒绝覆盖文件或经过符号链接。</li>
     *     <li>编码owner header并证明terminal reserve之外仍有candidate空间。</li>
     *     <li>以CREATE_NEW打开文件，完整写入并force metadata/data后才返回OPEN实例。</li>
     * </ol>
     *
     * @param path 由operation id在受控目录派生的新文件
     * @param header 包含capture/table/version/protocol与manifest的不可变owner
     * @param maxBytes header、candidate和terminal证据的总容量
     * @param terminalReserveBytes candidate不能占用的终止预留，至少256字节
     * @return header已durable且尚未开始generation的日志
     * @throws DatabaseRuntimeException 路径、创建、写入或force失败时抛出，原始cause被保留
     */
    public static FileOnlineAlterChangeLog create(Path path, OnlineAlterLogHeader header,
                                                   long maxBytes, int terminalReserveBytes) {
        validateCapacity(maxBytes, terminalReserveBytes);
        if (header == null) {
            throw new DatabaseValidationException("online ALTER log header must not be null");
        }
        Path normalized = preparePath(path);
        byte[] encoded = new OnlineAlterRowLogCodec().encodeHeader(header);
        if (encoded.length + terminalReserveBytes >= maxBytes) {
            throw new DatabaseValidationException(
                    "online ALTER manifest leaves no candidate capacity");
        }
        FileChannel opened = null;
        try {
            opened = FileChannel.open(normalized, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            writeFully(opened, ByteBuffer.wrap(encoded));
            opened.force(true);
            return new FileOnlineAlterChangeLog(normalized, opened, header,
                    encoded.length, maxBytes, terminalReserveBytes);
        } catch (IOException error) {
            closeOnFailure(opened, error);
            throw new DatabaseRuntimeException(
                    "create online ALTER journal failed: " + normalized, error);
        }
    }

    /**
     * 打开既有journal并恢复严格状态机；仅最后一个未完整写出的frame允许截断。
     *
     * @param path marker记录并由调用方约束在online-ddl目录内的既有文件
     * @param maxBytes 当前配置容量，不得小于既有文件
     * @param terminalReserveBytes 与创建时相同的终止预留
     * @return 完成owner/CRC/sequence/phase验证的日志
     * @throws DatabaseFatalException I/O、中间损坏、owner格式或状态序非法时抛出，恢复必须阻止OPEN
     */
    public static FileOnlineAlterChangeLog open(Path path, long maxBytes,
                                                 int terminalReserveBytes) {
        validateCapacity(maxBytes, terminalReserveBytes);
        Path normalized = requireExistingPath(path);
        FileChannel opened = null;
        try {
            opened = FileChannel.open(normalized, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            OnlineAlterRowLogCodec codec = new OnlineAlterRowLogCodec();
            int headerLength = codec.declaredHeaderLength(readExact(opened, 0,
                    OnlineAlterRowLogCodec.HEADER_PREFIX_BYTES));
            if (headerLength >= maxBytes) {
                throw new DatabaseValidationException(
                        "online ALTER journal header exceeds configured capacity");
            }
            OnlineAlterLogHeader header = codec.decodeHeader(readExact(opened, 0, headerLength));
            FileOnlineAlterChangeLog result = new FileOnlineAlterChangeLog(
                    normalized, opened, header, headerLength, maxBytes, terminalReserveBytes);
            result.scanAndRepairTail();
            return result;
        } catch (IOException | DatabaseRuntimeException error) {
            closeOnFailure(opened, error);
            if (error instanceof DatabaseFatalException fatal) {
                throw fatal;
            }
            throw new DatabaseFatalException(
                    "open online ALTER journal failed: " + normalized, error);
        }
    }

    @Override
    public OnlineAlterLogHeader header() {
        return header;
    }

    @Override
    public OnlineDdlCaptureId captureId() {
        return header.captureId();
    }

    @Override
    public Path path() {
        return path;
    }

    /**
     * 在聚簇物理修改前append opaque candidate；容量分支会在同一文件临界区留下durable abort。
     *
     * @param transactionId 已分配正write id的ACTIVE事务
     * @param payload capture codec生成的非空稳定身份或多目标payload
     * @return 正sequence；容量耗尽但abort证据已force时返回0，使业务DML可继续
     * @throws DatabaseRuntimeException 状态不允许、日志关闭或I/O失败时抛出
     */
    @Override
    public long appendCandidate(TransactionId transactionId, byte[] payload) {
        if (transactionId == null || transactionId.isNone()
                || payload == null || payload.length == 0) {
            throw new DatabaseValidationException(
                    "online ALTER candidate requires transaction id and non-empty payload");
        }
        stateLock.lock();
        try {
            requireWritable();
            if (!capturing || sealed || abortRequired || reconciled) {
                throw new DatabaseValidationException(
                        "online ALTER candidate requires an open CAPTURING interval");
            }
            OnlineAlterLogRecord record = new OnlineAlterLogRecord(
                    OnlineAlterLogRecordType.CANDIDATE, generation, nextSequence(),
                    transactionId.value(), payload);
            try {
                appendRecord(record, true);
                return record.sequence();
            } catch (OnlineAlterCapacityException exhausted) {
                persistAbortRequired(OnlineDdlAbortReason.ROW_LOG_CAPACITY);
                return 0;
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 追加coordinator phase frame。phase先校验、后写文件、最后发布内存状态，I/O失败不会虚构阶段。
     *
     * @param type generation/capturing/sealed/ready/reconciled之一
     * @param payload v1固定为空数组
     * @return 新frame sequence，需由调用方在阶段边界force
     */
    @Override
    public long appendState(OnlineAlterLogRecordType type, byte[] payload) {
        if (type == null || payload == null || type == OnlineAlterLogRecordType.CANDIDATE
                || type == OnlineAlterLogRecordType.FORCE_WATERMARK
                || type == OnlineAlterLogRecordType.ABORT_REQUIRED) {
            throw new DatabaseValidationException("online ALTER state frame is invalid");
        }
        stateLock.lock();
        try {
            requireWritable();
            validateStateAppend(type, payload);
            OnlineAlterLogRecord record = new OnlineAlterLogRecord(type, generation,
                    nextSequence(), 0, payload);
            appendRecord(record, true);
            publishState(type);
            return record.sequence();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 在正预算内取得文件锁并force覆盖目标sequence；force结果未知时实例进入fail-stop。
     *
     * @param sequence 不超过append high-water的正目标
     * @param timeout 文件锁与I/O协调的正等待预算
     */
    @Override
    public void forceThrough(long sequence, Duration timeout) {
        validateWait(sequence, timeout);
        acquireWithin(timeout, "force");
        try {
            requireWritable();
            if (sequence > highestAppended) {
                throw new DatabaseValidationException(
                        "online ALTER force target exceeds append high-water: " + sequence);
            }
            if (sequence <= highestForced) {
                return;
            }
            long forceTarget = highestAppended;
            byte[] payload = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                    .putLong(forceTarget).array();
            OnlineAlterLogRecord watermark = new OnlineAlterLogRecord(
                    OnlineAlterLogRecordType.FORCE_WATERMARK, generation,
                    nextSequence(), 0, payload);
            appendRecord(watermark, false);
            try {
                channel.force(false);
                highestForced = watermark.sequence();
            } catch (IOException error) {
                failed = true;
                throw new DatabaseFatalException(
                        "force online ALTER journal failed: " + path, error);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 幂等写入并force存储层终止原因；该frame使用candidate不可触及的terminal reserve。
     *
     * @param reason 稳定终止分类
     * @param timeout 正等待预算；当前实现持短内存锁，保留参数以稳定端口
     */
    @Override
    public void markAbortRequired(OnlineDdlAbortReason reason, Duration timeout) {
        if (reason == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "online ALTER abort requires reason and positive timeout");
        }
        stateLock.lock();
        try {
            requireWritable();
            if (!abortRequired) {
                persistAbortRequired(reason);
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public boolean abortRequired() {
        stateLock.lock();
        try {
            return abortRequired;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public long highestAppendedSequence() {
        stateLock.lock();
        try {
            return highestAppended;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public long highestForcedSequence() {
        stateLock.lock();
        try {
            return highestForced;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public long sizeBytes() {
        stateLock.lock();
        try {
            requireOpen();
            return currentSizeBytes;
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public OnlineChangeLogSnapshot snapshot() {
        stateLock.lock();
        try {
            return new OnlineChangeLogSnapshot(generation, candidateCount,
                    currentSizeBytes, maxBytes, terminalReserveBytes,
                    highestAppended, highestForced, abortRequired, failed, closed,
                    capturing, sealed, reconciled);
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public List<OnlineAlterLogRecord> readAll() {
        stateLock.lock();
        try {
            return List.copyOf(records);
        } finally {
            stateLock.unlock();
        }
    }

    /** 有界复制candidate窗口；内部仍在同一状态锁下读取已校验索引，返回后不持文件或锁资源。 */
    @Override
    public List<OnlineAlterLogRecord> readCandidatesAfter(
            long sequenceExclusive, int limit) {
        if (sequenceExclusive < 0 || limit <= 0) {
            throw new DatabaseValidationException(
                    "online ALTER candidate batch cursor/limit is invalid");
        }
        stateLock.lock();
        try {
            requireOpen();
            return records.stream()
                    .filter(record -> record.sequence() > sequenceExclusive
                            && record.type() == OnlineAlterLogRecordType.CANDIDATE)
                    .limit(limit).toList();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * recovery关闭流量时截断旧frame并force，随后以更高generation从空sequence重建。
     *
     * @param timeout 正恢复预算
     */
    @Override
    public void resetToManifest(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("online ALTER reset requires positive timeout");
        }
        stateLock.lock();
        try {
            requireWritable();
            try {
                channel.truncate(headerBytes);
                channel.position(headerBytes);
                channel.force(false);
            } catch (IOException error) {
                failed = true;
                throw new DatabaseFatalException(
                        "reset online ALTER journal failed: " + path, error);
            }
            records.clear();
            candidateCount = 0;
            currentSizeBytes = headerBytes;
            highestAppended = 0;
            highestForced = 0;
            generationStarted = false;
            capturing = false;
            sealed = false;
            readyToPublish = false;
            reconciled = false;
            abortRequired = false;
            if (generation == Long.MAX_VALUE) {
                failed = true;
                throw new DatabaseFatalException(
                        "online ALTER journal generation exhausted: " + path);
            }
            generation++;
        } finally {
            stateLock.unlock();
        }
    }

    /** 关闭自有句柄但保留文件，重复调用幂等。 */
    @Override
    public void close() {
        stateLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException error) {
                throw new DatabaseRuntimeException(
                        "close online ALTER journal failed: " + path, error);
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * 顺序扫描frame并恢复状态投影；只有长度尚未满足的最后frame可以截断。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取文件边界并使用局部变量构建候选状态，避免损坏扫描污染公开快照。</li>
     *     <li>逐frame校验长度、CRC、连续sequence、单generation与phase前驱。</li>
     *     <li>完整成功后一次发布投影并把channel定位到有效尾部。</li>
     * </ol>
     *
     * @throws IOException 文件读取或安全截断失败时抛出给open统一包装
     * @throws DatabaseFatalException 完整frame或状态序损坏时阻止恢复OPEN
     */
    private void scanAndRepairTail() throws IOException {
        // 1. 先建立局部状态，完整验证前不暴露部分恢复结果。
        long fileSize = channel.size();
        if (fileSize > maxBytes) {
            throw new DatabaseFatalException(
                    "online ALTER journal exceeds configured capacity: " + path);
        }
        long offset = headerBytes;
        long previous = 0;
        long fileGeneration = 0;
        long scannedCandidates = 0;
        boolean scannedStarted = false;
        boolean scannedCapturing = false;
        boolean scannedSealed = false;
        boolean scannedReady = false;
        boolean scannedReconciled = false;
        boolean scannedAborted = false;

        // 2. 仅物理不完整尾部可截断；完整CRC或phase错误都必须fail-closed。
        while (offset < fileSize) {
            long remaining = fileSize - offset;
            if (remaining < Integer.BYTES) {
                fileSize = truncateTail(offset);
                break;
            }
            int length = readExact(channel, offset, Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN).getInt();
            if (length < OnlineAlterRowLogCodec.MIN_FRAME_BYTES
                    || length > OnlineAlterRowLogCodec.MAX_FRAME_BYTES) {
                throw new DatabaseFatalException(
                        "online ALTER journal frame length is invalid at offset " + offset);
            }
            if (remaining < length) {
                fileSize = truncateTail(offset);
                break;
            }
            OnlineAlterLogRecord record;
            try {
                record = codec.decodeRecord(readExact(channel, offset, length));
            } catch (DatabaseRuntimeException error) {
                throw new DatabaseFatalException(
                        "online ALTER journal complete frame is corrupted at offset " + offset,
                        error);
            }
            if (record.sequence() != previous + 1) {
                throw invalidState(offset, record, "non-contiguous sequence");
            }
            if (fileGeneration == 0) {
                fileGeneration = record.generation();
            } else if (fileGeneration != record.generation()) {
                throw invalidState(offset, record, "mixed generation");
            }
            switch (record.type()) {
                case GENERATION_STARTED -> {
                    if (scannedStarted || previous != 0 || record.payload().length != 0) {
                        throw invalidState(offset, record, "duplicate/non-first generation");
                    }
                    scannedStarted = true;
                }
                case CAPTURING -> {
                    if (!scannedStarted || scannedCapturing || scannedSealed
                            || scannedAborted || record.payload().length != 0) {
                        throw invalidState(offset, record, "CAPTURING without open generation");
                    }
                    scannedCapturing = true;
                }
                case CANDIDATE -> {
                    if (!scannedCapturing || scannedSealed || scannedAborted
                            || record.payload().length == 0) {
                        throw invalidState(offset, record, "candidate outside capture");
                    }
                    scannedCandidates++;
                }
                case SEALED -> {
                    if (!scannedCapturing || scannedSealed || scannedAborted
                            || record.payload().length != 0) {
                        throw invalidState(offset, record, "SEALED without capture");
                    }
                    scannedSealed = true;
                }
                case READY_TO_PUBLISH -> {
                    if (!scannedSealed || scannedReady || scannedAborted
                            || record.payload().length != 0
                            && record.payload().length != Long.BYTES) {
                        throw invalidState(offset, record, "READY without unique seal");
                    }
                    if (record.payload().length == Long.BYTES
                            && ByteBuffer.wrap(record.payload()).order(ByteOrder.BIG_ENDIAN)
                            .getLong() < header.freezeReadViewGeneration()) {
                        throw invalidState(offset, record,
                                "READY read-view fence precedes capture baseline");
                    }
                    scannedReady = true;
                }
                case RECONCILED -> {
                    if (!scannedReady || scannedReconciled || scannedAborted
                            || record.payload().length != 0) {
                        throw invalidState(offset, record, "RECONCILED without READY");
                    }
                    scannedReconciled = true;
                }
                case ABORT_REQUIRED -> {
                    if (scannedAborted || scannedReconciled || record.payload().length == 0) {
                        throw invalidState(offset, record, "duplicate/late abort");
                    }
                    decodeAbortReason(record, offset);
                    scannedAborted = true;
                }
                case FORCE_WATERMARK -> validateWatermark(record, previous, offset);
            }
            previous = record.sequence();
            if (record.type() == OnlineAlterLogRecordType.FORCE_WATERMARK
                    || record.type() == OnlineAlterLogRecordType.ABORT_REQUIRED) {
                highestForced = record.sequence();
            }
            records.add(record);
            offset += length;
        }

        // 3. 扫描成功后原子发布内存投影，channel从确认过的尾部继续append。
        generation = fileGeneration == 0 ? generation : fileGeneration;
        highestAppended = previous;
        generationStarted = scannedStarted;
        capturing = scannedCapturing;
        sealed = scannedSealed;
        readyToPublish = scannedReady;
        reconciled = scannedReconciled;
        abortRequired = scannedAborted;
        candidateCount = scannedCandidates;
        currentSizeBytes = fileSize;
        channel.position(fileSize);
    }

    /** append前验证固定phase前驱，避免产生CRC正确但恢复不可解释的日志。 */
    private void validateStateAppend(OnlineAlterLogRecordType type, byte[] payload) {
        if (payload.length != 0
                && (type != OnlineAlterLogRecordType.READY_TO_PUBLISH
                || payload.length != Long.BYTES)) {
            throw new DatabaseValidationException(
                    "online ALTER v1 state payload is invalid");
        }
        switch (type) {
            case GENERATION_STARTED -> {
                if (generationStarted || highestAppended != 0) {
                    throw new DatabaseValidationException(
                            "online ALTER generation start must be first");
                }
            }
            case CAPTURING -> {
                if (!generationStarted || capturing || sealed || abortRequired) {
                    throw new DatabaseValidationException(
                            "online ALTER CAPTURING requires open generation");
                }
            }
            case SEALED -> {
                if (!capturing || sealed || abortRequired) {
                    throw new DatabaseValidationException(
                            "online ALTER SEALED requires open capture");
                }
            }
            case READY_TO_PUBLISH -> {
                if (!sealed || readyToPublish || abortRequired) {
                    throw new DatabaseValidationException(
                            "online ALTER READY requires unique seal");
                }
                if (payload.length == Long.BYTES
                        && ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong()
                        < header.freezeReadViewGeneration()) {
                    throw new DatabaseValidationException(
                            "online ALTER READY read-view fence precedes capture baseline");
                }
            }
            case RECONCILED -> {
                if (!readyToPublish || reconciled || abortRequired) {
                    throw new DatabaseValidationException(
                            "online ALTER RECONCILED requires READY");
                }
            }
            case CANDIDATE, FORCE_WATERMARK, ABORT_REQUIRED ->
                    throw new DatabaseValidationException(
                            "candidate/watermark/abort has a dedicated API");
        }
    }

    /** frame完整写入后才更新phase投影。 */
    private void publishState(OnlineAlterLogRecordType type) {
        switch (type) {
            case GENERATION_STARTED -> generationStarted = true;
            case CAPTURING -> capturing = true;
            case SEALED -> sealed = true;
            case READY_TO_PUBLISH -> readyToPublish = true;
            case RECONCILED -> reconciled = true;
            case CANDIDATE, FORCE_WATERMARK, ABORT_REQUIRED -> {
                // 这些类型不从state API发布。
            }
        }
    }

    /** 在锁内编码并完整写入单frame，成功后更新sequence/size/count。 */
    private void appendRecord(OnlineAlterLogRecord record, boolean preserveTerminalReserve) {
        byte[] encoded = codec.encodeRecord(record);
        long limit = preserveTerminalReserve ? maxBytes - terminalReserveBytes : maxBytes;
        if (encoded.length > limit - currentSizeBytes) {
            throw new OnlineAlterCapacityException(
                    "online ALTER journal capacity exhausted: " + path);
        }
        try {
            channel.position(currentSizeBytes);
            writeFully(channel, ByteBuffer.wrap(encoded));
            records.add(record);
            currentSizeBytes = Math.addExact(currentSizeBytes, encoded.length);
            highestAppended = record.sequence();
            if (record.type() == OnlineAlterLogRecordType.CANDIDATE) {
                candidateCount = Math.addExact(candidateCount, 1);
            }
        } catch (IOException error) {
            failed = true;
            throw new DatabaseFatalException(
                    "append online ALTER journal failed: " + path, error);
        } catch (ArithmeticException overflow) {
            failed = true;
            throw new DatabaseFatalException(
                    "online ALTER journal counters overflow: " + path, overflow);
        }
    }

    /** 使用terminal reserve追加并force唯一ABORT_REQUIRED。 */
    private void persistAbortRequired(OnlineDdlAbortReason reason) {
        if (reconciled) {
            throw new DatabaseFatalException(
                    "online ALTER cannot abort after reconciliation: " + path);
        }
        OnlineAlterLogRecord record = new OnlineAlterLogRecord(
                OnlineAlterLogRecordType.ABORT_REQUIRED, generation, nextSequence(), 0,
                reason.name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try {
            appendRecord(record, false);
            abortRequired = true;
            channel.force(false);
            highestForced = record.sequence();
        } catch (IOException | DatabaseRuntimeException error) {
            failed = true;
            if (error instanceof DatabaseFatalException fatal) {
                throw fatal;
            }
            throw new DatabaseFatalException(
                    "persist online ALTER abort failed: " + path, error);
        }
    }

    private long nextSequence() {
        if (highestAppended == Long.MAX_VALUE) {
            throw new DatabaseFatalException(
                    "online ALTER journal sequence exhausted: " + path);
        }
        return highestAppended + 1;
    }

    private void requireOpen() {
        if (closed) {
            throw new DatabaseRuntimeException(
                    "online ALTER journal is closed: " + path);
        }
    }

    private void requireWritable() {
        requireOpen();
        if (failed) {
            throw new DatabaseFatalException(
                    "online ALTER journal is fail-stopped: " + path);
        }
    }

    private void acquireWithin(Duration timeout, String action) {
        try {
            if (!stateLock.tryLock(boundedTimeoutNanos(timeout), TimeUnit.NANOSECONDS)) {
                throw new DatabaseRuntimeException(
                        action + " online ALTER journal timed out: " + path);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException(
                    action + " online ALTER journal interrupted: " + path, interrupted);
        }
    }

    private static void validateWait(long sequence, Duration timeout) {
        if (sequence <= 0 || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "online ALTER force requires positive sequence and timeout");
        }
    }

    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static void validateCapacity(long maxBytes, int reserveBytes) {
        if (maxBytes <= 0 || reserveBytes < MIN_TERMINAL_RESERVE_BYTES
                || maxBytes <= reserveBytes) {
            throw new DatabaseValidationException(
                    "online ALTER journal capacity/reserve is invalid");
        }
    }

    private long truncateTail(long offset) throws IOException {
        channel.truncate(offset);
        channel.force(false);
        return offset;
    }

    private static void validateWatermark(OnlineAlterLogRecord record,
                                          long previous, long offset) {
        if (previous == 0 || record.payload().length != Long.BYTES) {
            throw invalidState(offset, record, "watermark without append history");
        }
        long target = ByteBuffer.wrap(record.payload()).order(ByteOrder.BIG_ENDIAN).getLong();
        if (target <= 0 || target > previous) {
            throw invalidState(offset, record, "watermark target outside append history");
        }
    }

    private static void decodeAbortReason(OnlineAlterLogRecord record, long offset) {
        try {
            OnlineDdlAbortReason.valueOf(new String(record.payload(),
                    java.nio.charset.StandardCharsets.UTF_8));
        } catch (IllegalArgumentException unknown) {
            throw new DatabaseFatalException(
                    "online ALTER journal unknown abort reason at offset " + offset, unknown);
        }
    }

    private static DatabaseFatalException invalidState(
            long offset, OnlineAlterLogRecord record, String reason) {
        return new DatabaseFatalException(
                "online ALTER journal state invalid at offset " + offset
                        + ": type=" + record.type() + " reason=" + reason);
    }

    private static Path preparePath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException(
                    "online ALTER journal path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        Path parent = normalized.getParent();
        if (parent == null) {
            throw new DatabaseValidationException(
                    "online ALTER journal requires parent directory");
        }
        try {
            Files.createDirectories(parent);
            if (Files.isSymbolicLink(parent)
                    || Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
                throw new DatabaseValidationException(
                        "online ALTER journal target must be new non-symlink file: " + normalized);
            }
            return normalized;
        } catch (IOException error) {
            throw new DatabaseRuntimeException(
                    "prepare online ALTER journal path failed: " + normalized, error);
        }
    }

    private static Path requireExistingPath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException(
                    "online ALTER journal path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(normalized) || normalized.getParent() == null
                || Files.isSymbolicLink(normalized.getParent())) {
            throw new DatabaseValidationException(
                    "online ALTER journal must be regular non-symlink file: " + normalized);
        }
        return normalized;
    }

    private static void writeFully(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            int written = channel.write(source);
            if (written <= 0) {
                throw new IOException("online ALTER journal write made no progress");
            }
        }
    }

    private static ByteBuffer readExact(FileChannel channel, long position,
                                        int length) throws IOException {
        ByteBuffer result = ByteBuffer.allocate(length);
        long current = position;
        while (result.hasRemaining()) {
            int read = channel.read(result, current);
            if (read < 0) {
                throw new IOException(
                        "online ALTER journal truncated at offset " + current);
            }
            if (read == 0) {
                throw new IOException(
                        "online ALTER journal read made no progress at offset " + current);
            }
            current += read;
        }
        result.flip();
        return result;
    }

    private static void closeOnFailure(FileChannel channel, Throwable original) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException closeFailure) {
            original.addSuppressed(closeFailure);
        }
    }

    /** candidate容量越界的内部信号，只能在同一锁内转换为durable abort。 */
    private static final class OnlineAlterCapacityException extends DatabaseRuntimeException {
        private OnlineAlterCapacityException(String message) {
            super(message);
        }
    }
}
