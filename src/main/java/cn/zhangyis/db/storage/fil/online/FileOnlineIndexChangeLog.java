package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlAbortReason;
import cn.zhangyis.db.storage.api.ddl.online.OnlineChangeLogSnapshot;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexChangeLog;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecordType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单个 Online ADD INDEX 的受控 FileChannel row log。实例独占一个文件句柄；显式锁保护append顺序、force
 * 高水位、abort和close状态，不与其它build共享全局锁。
 */
public final class FileOnlineIndexChangeLog implements OnlineIndexChangeLog {

    /** 文件内容codec，无可变状态。 */
    private final OnlineIndexRowLogCodec codec = new OnlineIndexRowLogCodec();
    /** 规范化精确文件路径；删除和诊断只能使用本值。 */
    private final Path path;
    /** 当前实例独占的读写channel。 */
    private final FileChannel channel;
    /** 文件offset 0的immutable owner/manifest。 */
    private final OnlineIndexLogHeader header;
    /** reset generation时保留的header精确字节长度。 */
    private final long headerBytes;
    /** 文件总上限。 */
    private final long maxBytes;
    /** candidate不可消费的terminal reserve。 */
    private final int abortReserveBytes;
    /** 保护下列append/force/terminal状态。 */
    private final ReentrantLock lock = new ReentrantLock(true);
    /** 已解码/追加frame的有序内存投影，只在lock内修改。 */
    private final List<OnlineIndexLogRecord> records = new ArrayList<>();

    /** 当前generation；v1 create/reopen从最后frame恢复，reset后递增。 */
    private long generation = 1;
    /** 当前 generation 的 candidate frame 数；避免 snapshot 扫描全部 records。 */
    private long candidateCount;
    /** 已知完整 header/frame 字节数；高频 snapshot 不调用 FileChannel.size。 */
    private long currentSizeBytes;
    /** 最后分配的严格递增sequence。 */
    private long highestAppended;
    /** 已由成功FileChannel.force覆盖的sequence。 */
    private long highestForced;
    /** 是否已经持久观察到ABORT_REQUIRED。 */
    private boolean abortRequired;
    /** 当前 generation 是否已追加唯一 GENERATION_STARTED；由文件锁保护。 */
    private boolean generationStarted;
    /** 当前 generation 是否已进入 candidate 可追加区间；由文件锁保护。 */
    private boolean capturing;
    /** 当前 generation 是否已封闭 candidate 区间；由文件锁保护。 */
    private boolean sealed;
    /** 当前 generation 是否已形成唯一发布候选；由文件锁保护。 */
    private boolean reconciled;
    /** I/O结果未知后禁止继续append。 */
    private boolean failed;
    /** channel生命周期终态。 */
    private boolean closed;

    private FileOnlineIndexChangeLog(Path path, FileChannel channel, OnlineIndexLogHeader header,
                                     long headerBytes, long maxBytes, int abortReserveBytes) {
        this.path = path;
        this.channel = channel;
        this.header = header;
        this.headerBytes = headerBytes;
        this.maxBytes = maxBytes;
        this.abortReserveBytes = abortReserveBytes;
        this.currentSizeBytes = headerBytes;
    }

    /**
     * 创建全新日志并在返回前force immutable header/manifest。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>规范化路径并拒绝符号链接父目录，创建受控父目录。</li>
     *     <li>以CREATE_NEW打开独占channel，防止覆盖已有DDL证据。</li>
     *     <li>完整写header并force，保证后续DDL marker不会引用未持久manifest。</li>
     *     <li>构造OPEN实例；任一步失败关闭channel并保留原始cause。</li>
     * </ol>
     *
     * @param path 从baseDir/online-ddl和build id派生的目标文件
     * @param header immutable owner/manifest
     * @param maxBytes 文件总容量
     * @param abortReserveBytes terminal abort预留
     * @return header已durable的OPEN日志
     * @throws DatabaseRuntimeException 文件创建或force失败时抛出
     */
    public static FileOnlineIndexChangeLog create(Path path, OnlineIndexLogHeader header,
                                                  long maxBytes, int abortReserveBytes) {
        validateCapacity(maxBytes, abortReserveBytes);
        Path normalized = preparePath(path);
        OnlineIndexRowLogCodec codec = new OnlineIndexRowLogCodec();
        byte[] encoded = codec.encodeHeader(header);
        if (encoded.length + abortReserveBytes >= maxBytes) {
            throw new DatabaseValidationException("online index manifest leaves no candidate capacity");
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(normalized, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            writeFully(channel, ByteBuffer.wrap(encoded));
            channel.force(true);
            return new FileOnlineIndexChangeLog(normalized, channel, header,
                    encoded.length, maxBytes, abortReserveBytes);
        } catch (IOException error) {
            closeOnFailure(channel, error);
            throw new DatabaseRuntimeException("create online index row log failed: " + normalized, error);
        }
    }

    /**
     * 打开既有日志，严格扫描完整frame并只截断最后一个未完成frame。
     *
     * @param path 已由DDL marker提供并经受控目录验证的文件
     * @param maxBytes 当前实例配置容量，不能小于现有文件
     * @param abortReserveBytes terminal reserve
     * @return 恢复highest sequence/force/abort状态的OPEN日志
     * @throws DatabaseFatalException 中间frame损坏或I/O失败时抛出
     */
    public static FileOnlineIndexChangeLog open(Path path, long maxBytes, int abortReserveBytes) {
        validateCapacity(maxBytes, abortReserveBytes);
        Path normalized = requireExistingPath(path);
        FileChannel channel = null;
        try {
            channel = FileChannel.open(normalized, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            OnlineIndexRowLogCodec codec = new OnlineIndexRowLogCodec();
            ByteBuffer prefix = readExact(channel, 0, OnlineIndexRowLogCodec.HEADER_PREFIX_BYTES);
            int headerLength = codec.declaredHeaderLength(prefix);
            if (headerLength >= maxBytes) {
                throw new DatabaseValidationException("online index row log header exceeds configured capacity");
            }
            OnlineIndexLogHeader header = codec.decodeHeader(readExact(channel, 0, headerLength));
            FileOnlineIndexChangeLog result = new FileOnlineIndexChangeLog(
                    normalized, channel, header, headerLength, maxBytes, abortReserveBytes);
            result.scanAndRepairTail();
            return result;
        } catch (IOException | DatabaseRuntimeException error) {
            closeOnFailure(channel, error);
            if (error instanceof DatabaseFatalException fatal) {
                throw fatal;
            }
            throw new DatabaseFatalException("open online index row log failed: " + normalized, error);
        }
    }

    @Override
    public OnlineIndexLogHeader header() {
        return header;
    }

    @Override
    public Path path() {
        return path;
    }

    /**
     * 在物理DML前追加candidate；本方法只执行有界channel write，不force。
     *
     * @param transactionId 已由TransactionManager分配的正write id
     * @param payload SecondaryIndexLayout投影后的稳定opaque entry编码
     * @return 本candidate的严格递增sequence，commit/prepare必须force覆盖它
     * @throws DatabaseRuntimeException 容量不足或I/O失败时抛出；容量分支仍保留abort reserve
     */
    @Override
    public long appendCandidate(TransactionId transactionId, byte[] payload) {
        if (transactionId == null || transactionId.isNone() || payload == null) {
            throw new DatabaseValidationException("online index candidate requires transaction id and payload");
        }
        lock.lock();
        try {
            requireWritable();
            if (abortRequired) {
                throw new DatabaseRuntimeException("online index build already requires abort: " + path);
            }
            if (!capturing || sealed || reconciled) {
                throw new DatabaseValidationException(
                        "online index candidate append requires an open CAPTURING interval");
            }
            OnlineIndexLogRecord record = new OnlineIndexLogRecord(
                    OnlineIndexLogRecordType.CANDIDATE, generation,
                    nextSequence(), transactionId.value(), payload);
            try {
                appendRecord(record, true);
                return record.sequence();
            } catch (OnlineIndexRowLogCapacityException capacity) {
                // candidate 不得占用 terminal reserve；容量越界必须在释放文件锁前留下 durable abort 证据。
                persistAbortRequired(OnlineDdlAbortReason.ROW_LOG_CAPACITY);
                return 0;
            }
        } finally {
            lock.unlock();
        }
    }

    /** 状态frame只允许coordinator使用0 transaction identity追加。 */
    @Override
    public long appendState(OnlineIndexLogRecordType type, byte[] payload) {
        if (type == null || type == OnlineIndexLogRecordType.CANDIDATE
                || type == OnlineIndexLogRecordType.FORCE_WATERMARK
                || type == OnlineIndexLogRecordType.ABORT_REQUIRED || payload == null) {
            throw new DatabaseValidationException("online index state frame type/payload is invalid");
        }
        lock.lock();
        try {
            requireWritable();
            validateStateAppend(type, payload);
            OnlineIndexLogRecord record = new OnlineIndexLogRecord(
                    type, generation, nextSequence(), 0, payload);
            appendRecord(record, type != OnlineIndexLogRecordType.ABORT_REQUIRED);
            publishState(type);
            if (type == OnlineIndexLogRecordType.ABORT_REQUIRED) {
                abortRequired = true;
            }
            return record.sequence();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 强制文件覆盖目标sequence。v1在per-file显式锁内完成leader force，后续可在不改变接口的前提下让follower
     * 通过Condition合并；任何失败都把实例标为failed，禁止未知尾部后继续append。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在调用方预算内取得文件锁，校验目标不超过当前 append high-water，并识别已覆盖请求。</li>
     *     <li>追加覆盖当前全部记录的 FORCE_WATERMARK 后调用 FileChannel.force；成功才推进 forced high-water。</li>
     * </ol>
     */
    @Override
    public void forceThrough(long sequence, Duration timeout) {
        // 1. 文件锁等待与 force 共用调用方预算；进入 I/O 前拒绝未来 sequence 和已失败实例。
        validateWait(sequence, timeout);
        acquireWithin(timeout, "force");
        try {
            requireWritable();
            if (sequence > highestAppended) {
                throw new DatabaseValidationException(
                        "online index force target exceeds appended high-water: " + sequence);
            }
            if (sequence <= highestForced) {
                return;
            }
            // 2. leader 覆盖取得锁时的全部 append；失败进入 fail-stop，follower 不会误读 durable 结果。
            long forceTarget = highestAppended;
            OnlineIndexLogRecord watermark = new OnlineIndexLogRecord(
                    OnlineIndexLogRecordType.FORCE_WATERMARK, generation,
                    nextSequence(), 0, ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.BIG_ENDIAN).putLong(forceTarget).array());
            appendRecord(watermark, false);
            try {
                channel.force(false);
                highestForced = watermark.sequence();
            } catch (IOException error) {
                failed = true;
                throw new DatabaseFatalException(
                        "force online index row log failed: " + path + " target=" + sequence, error);
            }
        } finally {
            lock.unlock();
        }
    }

    /** ABORT_REQUIRED使用独立reserve并在返回前force。重复调用幂等。 */
    @Override
    public void markAbortRequired(OnlineDdlAbortReason reason, Duration timeout) {
        if (reason == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("online index abort requires reason and positive timeout");
        }
        lock.lock();
        try {
            requireWritable();
            if (abortRequired) {
                return;
            }
            persistAbortRequired(reason);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean abortRequired() {
        lock.lock();
        try {
            return abortRequired;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long highestAppendedSequence() {
        lock.lock();
        try {
            return highestAppended;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long highestForcedSequence() {
        lock.lock();
        try {
            return highestForced;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long sizeBytes() {
        lock.lock();
        try {
            requireOpen();
            return currentSizeBytes;
        } finally {
            lock.unlock();
        }
    }

    /** 单次文件锁内复制全部诊断标量；不进入 channel I/O，不复制无界 frame payload。 */
    @Override
    public OnlineChangeLogSnapshot snapshot() {
        lock.lock();
        try {
            return new OnlineChangeLogSnapshot(generation, candidateCount,
                    currentSizeBytes, maxBytes, abortReserveBytes,
                    highestAppended, highestForced, abortRequired, failed, closed,
                    capturing, sealed, reconciled);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<OnlineIndexLogRecord> readAll() {
        lock.lock();
        try {
            return List.copyOf(records);
        } finally {
            lock.unlock();
        }
    }

    /**
     * recovery流量关闭期丢弃旧generation frame，只保留immutable header/manifest并force truncate。
     */
    @Override
    public void resetToManifest(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("online index reset requires positive timeout");
        }
        lock.lock();
        try {
            requireWritable();
            try {
                channel.truncate(headerBytes);
                channel.position(headerBytes);
                channel.force(false);
            } catch (IOException error) {
                failed = true;
                throw new DatabaseFatalException("reset online index row log generation failed: " + path, error);
            }
            records.clear();
            candidateCount = 0;
            currentSizeBytes = headerBytes;
            highestAppended = 0;
            highestForced = 0;
            abortRequired = false;
            generationStarted = false;
            capturing = false;
            sealed = false;
            reconciled = false;
            generation++;
        } finally {
            lock.unlock();
        }
    }

    /** 关闭自有channel；重复close为no-op，不能清除持久文件。 */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException error) {
                throw new DatabaseRuntimeException("close online index row log failed: " + path, error);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 文件 open 时顺序恢复 frame；只有最后声明长度尚未满足的 frame 允许 truncate。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验文件容量并初始化只属于本次扫描的 generation/phase 投影。</li>
     *     <li>逐 frame 校验长度、CRC、连续 sequence、单 generation 与状态机；仅不完整尾 frame 可截断并 force。</li>
     *     <li>完整扫描成功后一次性发布恢复出的 phase/high-water，并把 channel 定位到有效尾部。</li>
     * </ol>
     */
    private void scanAndRepairTail() throws IOException {
        // 1. 容量和局部状态先验证，不在完整扫描前发布 capturing/sealed/reconciled。
        long fileSize = channel.size();
        if (fileSize > maxBytes) {
            throw new DatabaseValidationException("online index row log exceeds configured capacity");
        }
        long offset = headerBytes;
        long previousSequence = 0;
        long fileGeneration = 0;
        boolean generationStarted = false;
        boolean capturing = false;
        boolean sealed = false;
        boolean reconciled = false;
        boolean aborted = false;
        long scannedCandidates = 0;

        // 2. 中间完整 frame 的任意格式或状态错误均 fail-closed；仅 torn tail 可安全截断。
        while (offset < fileSize) {
            long remaining = fileSize - offset;
            if (remaining < Integer.BYTES) {
                channel.truncate(offset);
                channel.force(false);
                fileSize = offset;
                break;
            }
            ByteBuffer lengthBuffer = readExact(channel, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
            int length = lengthBuffer.getInt();
            if (length < OnlineIndexRowLogCodec.MIN_FRAME_BYTES
                    || length > OnlineIndexRowLogCodec.MAX_FRAME_BYTES) {
                throw new DatabaseFatalException(
                        "online index row log has invalid middle frame length: offset=" + offset + " length=" + length);
            }
            if (remaining < length) {
                channel.truncate(offset);
                channel.force(false);
                fileSize = offset;
                break;
            }
            OnlineIndexLogRecord record;
            try {
                record = codec.decodeRecord(readExact(channel, offset, length));
            } catch (DatabaseRuntimeException error) {
                throw new DatabaseFatalException(
                        "online index row log has corrupted complete frame at offset " + offset, error);
            }
            if (record.sequence() != previousSequence + 1) {
                throw new DatabaseFatalException(
                        "online index row log sequence is not contiguous at offset " + offset);
            }
            if (fileGeneration == 0) {
                fileGeneration = record.generation();
            } else if (record.generation() != fileGeneration) {
                throw new DatabaseFatalException(
                        "online index row log mixes generations without manifest reset at offset " + offset);
            }
            switch (record.type()) {
                case GENERATION_STARTED -> {
                    if (generationStarted || previousSequence != 0 || record.payload().length != 0) {
                        throw invalidState(offset, record, "duplicate/non-first generation start");
                    }
                    generationStarted = true;
                }
                case CAPTURING -> {
                    if (!generationStarted || capturing || sealed || aborted
                            || record.payload().length != 0) {
                        throw invalidState(offset, record, "CAPTURING without open generation");
                    }
                    capturing = true;
                }
                case CANDIDATE -> {
                    if (!capturing || sealed || aborted || record.payload().length == 0) {
                        throw invalidState(offset, record, "candidate outside capturing interval");
                    }
                    scannedCandidates++;
                }
                case SEALED -> {
                    if (!capturing || sealed || aborted || record.payload().length != 0) {
                        throw invalidState(offset, record, "SEALED outside capturing interval");
                    }
                    sealed = true;
                }
                case RECONCILED -> {
                    if (!sealed || reconciled || aborted || record.payload().length != 0) {
                        throw invalidState(offset, record, "RECONCILED before unique seal");
                    }
                    reconciled = true;
                }
                case ABORT_REQUIRED -> {
                    if (aborted || reconciled || record.payload().length == 0) {
                        throw invalidState(offset, record, "duplicate/late/empty abort");
                    }
                    try {
                        OnlineDdlAbortReason.valueOf(
                                new String(record.payload(), StandardCharsets.UTF_8));
                    } catch (IllegalArgumentException unknownReason) {
                        throw new DatabaseFatalException(
                                "online index row log has unknown abort reason at offset " + offset,
                                unknownReason);
                    }
                    aborted = true;
                }
                case FORCE_WATERMARK -> {
                    if (previousSequence == 0 || record.payload().length != Long.BYTES) {
                        throw invalidState(offset, record, "force watermark has no target");
                    }
                    long target = ByteBuffer.wrap(record.payload())
                            .order(ByteOrder.BIG_ENDIAN).getLong();
                    if (target <= 0 || target > previousSequence) {
                        throw invalidState(offset, record,
                                "force watermark target is outside append history");
                    }
                }
            }
            previousSequence = record.sequence();
            generation = Math.max(generation, record.generation());
            highestAppended = record.sequence();
            if (record.type() == OnlineIndexLogRecordType.FORCE_WATERMARK
                    || record.type() == OnlineIndexLogRecordType.ABORT_REQUIRED) {
                highestForced = record.sequence();
            }
            if (record.type() == OnlineIndexLogRecordType.ABORT_REQUIRED) {
                abortRequired = true;
            }
            records.add(record);
            offset += length;
        }

        // 3. 所有完整 frame 已通过后再发布状态投影，避免损坏文件留下部分可观察恢复状态。
        this.generationStarted = generationStarted;
        this.capturing = capturing;
        this.sealed = sealed;
        this.reconciled = reconciled;
        this.candidateCount = scannedCandidates;
        this.currentSizeBytes = fileSize;
        channel.position(fileSize);
    }

    /** 构造带 offset/type 的稳定格式致命异常，避免恢复日志只暴露笼统 CRC 成功。 */
    private static DatabaseFatalException invalidState(
            long offset, OnlineIndexLogRecord record, String reason) {
        return new DatabaseFatalException(
                "online index row log state is invalid at offset " + offset
                        + ": type=" + record.type() + " reason=" + reason);
    }

    /** 在写盘前验证状态 frame 的当前 generation 前驱和固定 payload 语义。 */
    private void validateStateAppend(OnlineIndexLogRecordType type, byte[] payload) {
        boolean empty = payload.length == 0;
        switch (type) {
            case GENERATION_STARTED -> {
                if (generationStarted || highestAppended != 0 || !empty) {
                    throw new DatabaseValidationException(
                            "online index generation start must be the first empty frame");
                }
            }
            case CAPTURING -> {
                if (!generationStarted || capturing || sealed || abortRequired || !empty) {
                    throw new DatabaseValidationException(
                            "online index CAPTURING requires one open generation");
                }
            }
            case SEALED -> {
                if (!capturing || sealed || abortRequired || !empty) {
                    throw new DatabaseValidationException(
                            "online index SEALED requires an open capture interval");
                }
            }
            case RECONCILED -> {
                if (!sealed || reconciled || abortRequired || !empty) {
                    throw new DatabaseValidationException(
                            "online index RECONCILED requires one sealed generation");
                }
            }
            case ABORT_REQUIRED -> {
                if (abortRequired || reconciled || empty) {
                    throw new DatabaseValidationException(
                            "online index ABORT_REQUIRED must be unique, non-empty and pre-reconciled");
                }
            }
            case CANDIDATE, FORCE_WATERMARK -> throw new DatabaseValidationException(
                    "candidate/force watermark cannot be appended through state API");
        }
    }

    /** append 成功后在同一文件锁内发布状态；I/O 失败不会提前改变内存 phase。 */
    private void publishState(OnlineIndexLogRecordType type) {
        switch (type) {
            case GENERATION_STARTED -> generationStarted = true;
            case CAPTURING -> capturing = true;
            case SEALED -> sealed = true;
            case RECONCILED -> reconciled = true;
            case ABORT_REQUIRED -> abortRequired = true;
            case CANDIDATE, FORCE_WATERMARK -> {
                // 两类不通过 appendState 发布。
            }
        }
    }

    /** 分配下一sequence；溢出不能回绕破坏持久顺序。 */
    private long nextSequence() {
        if (highestAppended == Long.MAX_VALUE) {
            throw new DatabaseFatalException("online index row log sequence exhausted: " + path);
        }
        return highestAppended + 1;
    }

    /** 在lock内完整写frame并发布内存投影。 */
    private void appendRecord(OnlineIndexLogRecord record, boolean reserveAbortSpace) {
        byte[] encoded = codec.encodeRecord(record);
        try {
            long current = currentSizeBytes;
            long limit = reserveAbortSpace ? maxBytes - abortReserveBytes : maxBytes;
            if (encoded.length > limit - current) {
                throw new OnlineIndexRowLogCapacityException(
                        "online index row log capacity exhausted: path=" + path
                                + " current=" + current + " frame=" + encoded.length + " limit=" + limit);
            }
            channel.position(current);
            writeFully(channel, ByteBuffer.wrap(encoded));
            records.add(record);
            highestAppended = record.sequence();
            currentSizeBytes = Math.addExact(current, encoded.length);
            if (record.type() == OnlineIndexLogRecordType.CANDIDATE) {
                candidateCount = Math.addExact(candidateCount, 1);
            }
        } catch (IOException error) {
            failed = true;
            throw new DatabaseFatalException("append online index row log failed: " + path, error);
        } catch (ArithmeticException overflow) {
            failed = true;
            throw new DatabaseFatalException("online index row log counters overflow: " + path, overflow);
        }
    }

    /**
     * 在调用者持有文件锁时追加并强制 terminal abort frame；成功返回后 coordinator 可安全停止捕获，失败则
     * 把实例置为 fail-stop，禁止未知 durable 边界后继续写入。
     *
     * @param reason 触发终止的稳定原因；会以名称写入诊断 payload
     * @throws DatabaseFatalException reserve 不足或 force 失败、无法证明 abort durable 时抛出
     */
    private void persistAbortRequired(OnlineDdlAbortReason reason) {
        if (reconciled) {
            throw new DatabaseFatalException(
                    "online index build cannot abort after durable reconciliation: " + path);
        }
        OnlineIndexLogRecord abort = new OnlineIndexLogRecord(
                OnlineIndexLogRecordType.ABORT_REQUIRED, generation,
                nextSequence(), 0, reason.name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try {
            appendRecord(abort, false);
            abortRequired = true;
            channel.force(false);
            highestForced = abort.sequence();
        } catch (RuntimeException | IOException error) {
            failed = true;
            if (error instanceof DatabaseFatalException fatal) {
                throw fatal;
            }
            throw new DatabaseFatalException(
                    "persist online index abort state failed: " + path, error);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new DatabaseRuntimeException("online index row log is closed: " + path);
        }
    }

    private void requireWritable() {
        requireOpen();
        if (failed) {
            throw new DatabaseFatalException("online index row log is fail-stopped: " + path);
        }
    }

    private static void validateWait(long sequence, Duration timeout) {
        if (sequence <= 0 || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("online index force requires positive sequence and timeout");
        }
    }

    /** 在调用方 timeout 内取得文件状态锁；中断保留标志且不进入任何 channel I/O。 */
    private void acquireWithin(Duration timeout, String action) {
        try {
            if (!lock.tryLock(boundedTimeoutNanos(timeout), TimeUnit.NANOSECONDS)) {
                throw new DatabaseRuntimeException(
                        action + " online index row log timed out acquiring file lock: " + path);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException(
                    action + " online index row log interrupted acquiring file lock: " + path,
                    interrupted);
        }
    }

    /** 正Duration超出long纳秒范围时饱和，避免在进入文件锁等待前泄漏算术异常。 */
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
            throw new DatabaseValidationException("online index log capacity/reserve is invalid");
        }
    }

    private static Path preparePath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("online index row log path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        Path parent = normalized.getParent();
        if (parent == null) {
            throw new DatabaseValidationException("online index row log requires a parent directory");
        }
        try {
            Files.createDirectories(parent);
            if (Files.isSymbolicLink(parent) || Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
                throw new DatabaseValidationException(
                        "online index row log target must be a new non-symlink file: " + normalized);
            }
            return normalized;
        } catch (IOException error) {
            throw new DatabaseRuntimeException("prepare online index row log path failed: " + normalized, error);
        }
    }

    private static Path requireExistingPath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("online index row log path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(normalized)
                || normalized.getParent() == null || Files.isSymbolicLink(normalized.getParent())) {
            throw new DatabaseValidationException(
                    "online index row log must be an existing regular non-symlink file: " + normalized);
        }
        return normalized;
    }

    private static void writeFully(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            int written = channel.write(source);
            if (written <= 0) {
                throw new IOException("online index row log write made no progress");
            }
        }
    }

    private static ByteBuffer readExact(FileChannel channel, long position, int length) throws IOException {
        ByteBuffer result = ByteBuffer.allocate(length);
        long current = position;
        while (result.hasRemaining()) {
            int read = channel.read(result, current);
            if (read < 0) {
                throw new IOException("online index row log is truncated at offset " + current);
            }
            if (read == 0) {
                throw new IOException("online index row log read made no progress at offset " + current);
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

    /** candidate 容量越界的文件层内部信号；只允许上层 append 在同一临界区转换为 durable abort。 */
    private static final class OnlineIndexRowLogCapacityException extends DatabaseRuntimeException {

        private OnlineIndexRowLogCapacityException(String message) {
            super(message);
        }
    }
}
