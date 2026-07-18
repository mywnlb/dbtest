package cn.zhangyis.db.storage.fil.catalog;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogCorruptionException;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogPersistenceException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/**
 * {@code mysql.ibd} v1 的双头、追加式内部 catalog 文件实现。
 *
 * <p>文件前 8 KiB 是两个交替发布的 4 KiB header slot，之后是紧凑排列的变长 data/commit frame。
 * header 的 {@code committedLength} 是唯一可见性边界：批次 frame 全部写入并执行 data force 后，
 * 才写下一 generation header 并执行 metadata force。因此崩溃留下的未发布尾部会在下次 append 前截掉，
 * 不会被恢复为已提交字典数据。每个 frame 由 CRC32C 防止局部损坏，每批 data frame 再由 commit frame
 * 中的 SHA-256 封口；发布边界内的格式、顺序或摘要错误一律 fail-closed。</p>
 *
 * <p>实例用一把显式 {@link ReentrantLock} 串行化 append、内存快照读取和 close。锁只覆盖当前 catalog
 * 文件，不参与表空间/page latch 或事务锁顺序。该 teaching slice 不复用 InnoDB 系统表空间页格式、
 * buffer pool、redo 或 doublewrite；它保留稳定 {@link InternalCatalogStore} API，使后续可以替换为
 * 真正的 catalog B+Tree，而无需让 DD repository 接触 {@link FileChannel} 和物理帧。</p>
 */
public final class FileInternalCatalogStore implements InternalCatalogStore {

    /**
     * 单个 header slot 的固定字节数；双槽分别占据文件偏移 0 和 4096，损坏其中一槽时仍可读取另一槽。
     */
    static final int HEADER_SLOT_BYTES = 4096;

    /**
     * data frame 区的首字节偏移；所有 committed length 都不得小于该双 header 边界。
     */
    public static final long DATA_START = HEADER_SLOT_BYTES * 2L;

    /** 用于拒绝其它文件格式的 8 字节稳定魔数，ASCII 语义为 {@code MINIDDIB}。 */
    private static final long HEADER_MAGIC = 0x4D_49_4E_49_44_44_49_42L; // MINIDDIB

    /** 当前持久格式版本；未知版本不会按兼容格式猜测，而会使对应 header slot 无效。 */
    private static final int FORMAT_VERSION = 1;

    /** header CRC 字段在 4 KiB slot 中的固定偏移；CRC 只覆盖该偏移之前的字节。 */
    private static final int HEADER_CRC_OFFSET = HEADER_SLOT_BYTES - Integer.BYTES;

    /** 表示携带一个无语义 key/payload 记录的 frame 类型码。 */
    private static final byte DATA_FRAME = 1;

    /** 表示一个批次结束并携带 data frame 数量与 SHA-256 的 frame 类型码。 */
    private static final byte COMMIT_FRAME = 2;

    /** 每个 frame 在 body 之前持久化的 {@code bodyLength + bodyCrc32c} 固定长度。 */
    private static final int FRAME_PREFIX_BYTES = Integer.BYTES * 2;

    /** 单条记录 key 的持久格式上限，限制损坏长度字段导致的内存分配。 */
    private static final int MAX_KEY_BYTES = 256;

    /** 单条记录 payload 的持久格式上限，限制 catalog v1 单 frame 大小。 */
    private static final int MAX_PAYLOAD_BYTES = 1024;

    /** commit frame 中批次摘要的固定长度。 */
    private static final int SHA256_BYTES = 32;

    /**
     * 串行 append/header 发布、内存快照读取与 close 的实例锁。锁内只访问本 catalog 的 channel
     * 和权威内存视图，不回调 DD repository，也不获取表空间或 page latch。
     */
    private final ReentrantLock ioLock = new ReentrantLock();

    /**
     * 本 store 独占的读写 channel；所有帧 IO 使用显式位置，不依赖共享 channel position。
     * channel 的打开到关闭生命周期归当前实例所有。
     */
    private final FileChannel channel;

    /**
     * catalog 文件路径，仅用于创建父目录、打开文件和异常诊断；它不是文件身份或一致性校验字段。
     */
    private final Path path;

    /**
     * 最近成功发布的 header 内存镜像，是 append 起点和 committed length 查询的权威状态；
     * 构造后只在 {@link #ioLock} 下替换。
     */
    private Header current;

    /**
     * 启动时从 committed 边界完整校验并重建、append 发布后追加的批次视图；
     * 由 {@link #ioLock} 保护，对外始终返回不可变副本。
     */
    private final List<CatalogBatch> batches;

    /**
     * 用已经验证的物理状态构造可服务实例。
     *
     * @param path 已打开 catalog 的诊断路径
     * @param channel 由实例接管并在 {@link #close()} 中关闭的读写 channel
     * @param current 已通过双槽选择和边界校验的最新 header
     * @param batches 从该 header 的 committed 区域恢复出的完整批次
     */
    private FileInternalCatalogStore(Path path, FileChannel channel, Header current, List<CatalogBatch> batches) {
        this.path = path;
        this.channel = channel;
        this.current = current;
        this.batches = new ArrayList<>(batches);
    }

    /**
     * 新建空 catalog，或打开并校验已有 catalog。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验路径并创建父目录；此时尚未打开 channel，不会留下 catalog 内容。</li>
     *     <li>打开可读写 channel；已有非空文件进入双 header 与 committed frame 恢复，不会被初始化覆盖。</li>
     *     <li>新文件或零长度文件先截到双 header 边界，再写 generation 1 header 并
     *     {@code force(true)}，只有初始空状态 durable 后才发布实例。</li>
     *     <li>任一文件系统失败都包装为持久化异常；未成功构造的文件不会作为可用 store 返回。</li>
     * </ol>
     *
     * @param path catalog 文件路径；不能为空，父目录不存在时会创建
     * @return 已恢复或已完成初始 header 持久化的 catalog store
     * @throws DatabaseValidationException path 为空时抛出；不会执行文件 IO
     * @throws InternalCatalogCorruptionException 已有非空文件没有可用 header，或 committed 区域损坏时抛出
     * @throws InternalCatalogPersistenceException 创建目录、打开、初始化或 force 文件失败时抛出
     */
    public static FileInternalCatalogStore openOrCreate(Path path) {
        // 1. 在接触文件系统前拒绝空路径，并保证目标父目录存在。
        validatePath(path);
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            // 2. 已有非空文件必须走恢复校验，不能因 CREATE 选项而覆盖 durable catalog。
            boolean exists = Files.exists(path);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            if (exists && channel.size() > 0) {
                return openValidated(path, channel);
            }

            // 3. 空 catalog 先建立双槽物理边界，再 durable 首个 header，最后发布内存实例。
            channel.truncate(DATA_START);
            Header initial = new Header(1, DATA_START, 1);
            writeHeader(channel, initial);
            channel.force(true);
            return new FileInternalCatalogStore(path, channel, initial, List.of());
        } catch (IOException e) {
            // 4. 文件系统失败保留根因；调用方不能把该路径当作已经成功打开的 catalog。
            throw new InternalCatalogPersistenceException("open/create internal catalog failed: " + path, e);
        }
    }

    /**
     * 打开必须已经存在的 catalog，并恢复其 committed 批次。
     *
     * <p>该入口不会创建文件或初始化空内容。channel 打开成功后交给统一恢复校验；校验失败时
     * {@link #openValidated(Path, FileChannel)} 会关闭 channel，避免句柄泄漏。</p>
     *
     * @param path 已有 catalog 文件路径；不能为空
     * @return 完成 header 选择、边界校验和批次扫描的 store
     * @throws DatabaseValidationException path 为空时抛出
     * @throws InternalCatalogCorruptionException 文件 header 或 committed frame 损坏时抛出
     * @throws InternalCatalogPersistenceException 文件不存在、无法打开或读取失败时抛出
     */
    public static FileInternalCatalogStore openExisting(Path path) {
        validatePath(path);
        try {
            return openValidated(path, FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE));
        } catch (IOException e) {
            throw new InternalCatalogPersistenceException("open internal catalog failed: " + path, e);
        }
    }

    /**
     * 从已打开 channel 选择最新有效 header，并恢复其 committed 批次。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>分别解码两个 header slot；单槽损坏可由另一槽继续恢复，双槽无效则 fail-closed。</li>
     *     <li>按 generation 选择最新有效 header，并校验其 committed length 位于 data 起点与物理 EOF 之间。</li>
     *     <li>仅扫描该 header 发布的字节范围，验证 frame CRC、批次顺序、ordinal 和 SHA-256 后重建内存视图。</li>
     *     <li>成功时把 channel ownership 转交 store；失败时先关闭 channel，并把关闭失败作为 suppressed
     *     cause 保留，不泄漏文件句柄。</li>
     * </ol>
     *
     * @param path channel 对应的诊断路径
     * @param channel 已打开的读写 channel；成功后由返回实例接管，失败时由本方法关闭
     * @return 与最新有效 header 一致的 store
     * @throws InternalCatalogCorruptionException header、边界或 committed frame 不满足 v1 格式时抛出
     * @throws InternalCatalogPersistenceException 扫描发生底层 IO 失败时抛出
     */
    private static FileInternalCatalogStore openValidated(Path path, FileChannel channel) {
        try {
            // 1. 独立校验双槽；坏槽不遮蔽仍然有效的另一 generation。
            List<Header> headers = new ArrayList<>(2);
            decodeHeader(channel, 0).ifPresent(headers::add);
            decodeHeader(channel, 1).ifPresent(headers::add);
            Header latest = headers.stream().max(Comparator.comparingLong(Header::generation))
                    .orElseThrow(() -> new InternalCatalogCorruptionException(
                            "both internal catalog headers are invalid: " + path));

            // 2. header 不得发布到 data 区之前或物理 EOF 之后，否则不能安全确定恢复边界。
            if (latest.committedLength < DATA_START || latest.committedLength > channel.size()) {
                throw new InternalCatalogCorruptionException("internal catalog committed length is invalid: "
                        + latest.committedLength + ", file=" + channel.size());
            }

            // 3. 只信任最新 header 的 committed 区间；尾部 crash 数据不参与恢复。
            List<CatalogBatch> batches = scanCommitted(channel, latest);
            return new FileInternalCatalogStore(path, channel, latest, batches);
        } catch (RuntimeException | IOException failure) {
            // 4. 构造失败由本方法回收尚未转交的 channel，并保留 cleanup 失败供诊断。
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new InternalCatalogPersistenceException("validate internal catalog failed: " + path, failure);
        }
    }

    /**
     * 追加并持久发布一个不可分割的 catalog 批次。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在获取 IO 锁前校验批次边界，避免无效输入进入串行文件临界区。</li>
     *     <li>从当前 header 取得 sequence/committed length，并截掉该边界之后可能存在的 crash tail。</li>
     *     <li>按 ordinal 写入所有 data frame，同时累计批次 SHA-256，随后写 commit frame；每个 frame
     *     自带长度和 CRC32C。</li>
     *     <li>先 {@code force(false)} 固化 frame，再把下一 generation、结束边界和下一 sequence 写入
     *     交替 header slot，并 {@code force(true)} 发布 metadata，维护“header 不先于 data durable”的不变量。</li>
     *     <li>只有双 force 全部成功后才替换内存 header、追加不可变批次并返回 sequence；失败不会发布
     *     本次内存状态，下次 append 会从旧 committed length 清理未发布尾部。</li>
     * </ol>
     *
     * @param records 待原子发布的无语义 catalog 记录；列表及元素不能为空，至少一条，key/payload
     *                分别不得超过 v1 持久格式上限
     * @return 本批次的正数、严格单调 sequence
     * @throws DatabaseValidationException 批次为空、含空元素或字段超过格式上限时抛出；文件不发生变化
     * @throws InternalCatalogPersistenceException 截断、写 frame、force、generation/sequence 溢出或
     *                                             header 发布失败时抛出；调用方不得假定本批次已提交
     */
    @Override
    public long append(List<CatalogRecord> records) {
        // 1. 批次合法性在锁外完成；记录对象自身已经防御性复制 key/payload。
        validateRecords(records);
        ioLock.lock();
        try {
            // 2. current 是锁内权威提交点，先截掉其后的未发布 crash tail。
            long sequence = current.nextBatchSequence;
            long position = current.committedLength;
            try {
                channel.truncate(position);
                MessageDigest digest = sha256();

                // 3. data frame 的原始 body 同时进入逐帧 CRC 和批次摘要，commit frame 封闭该批次。
                int ordinal = 0;
                for (CatalogRecord record : records) {
                    byte[] body = dataBody(sequence, ordinal++, record);
                    digest.update(body);
                    position = writeFrame(channel, position, body);
                }
                byte[] commit = commitBody(sequence, records.size(), digest.digest());
                position = writeFrame(channel, position, commit);

                // 4. 严格遵循 data force 在前、header metadata force 在后，避免发布尚未 durable 的 frame。
                channel.force(false);
                Header next = new Header(Math.addExact(current.generation, 1), position,
                        Math.addExact(sequence, 1));
                writeHeader(channel, next);
                channel.force(true);

                // 5. 持久发布成功后再更新内存权威视图；外部读取不会观察到半批次。
                current = next;
                batches.add(new CatalogBatch(sequence, List.copyOf(records)));
                return sequence;
            } catch (IOException | ArithmeticException e) {
                throw new InternalCatalogPersistenceException("append internal catalog batch failed: " + path, e);
            }
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 读取当前实例已恢复或已追加成功的全部 committed 批次。
     *
     * @return 按 sequence 升序排列的不可变列表快照；无批次时返回空列表，不返回 {@code null}
     */
    @Override
    public List<CatalogBatch> readCommittedBatches() {
        ioLock.lock();
        try {
            return List.copyOf(batches);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 读取最近成功发布 header 所证明的 durable 文件边界。
     *
     * @return 从文件起点计算的 exclusive 字节偏移；最小值为 {@link #DATA_START}
     */
    @Override
    public long committedLength() {
        ioLock.lock();
        try {
            return current.committedLength;
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 在排斥 append 和快照读取的条件下关闭实例拥有的 channel。
     *
     * <p>方法不隐式 force 尚未发布的数据；append 自身负责 durability。关闭成功后实例不得再使用。</p>
     *
     * @throws InternalCatalogPersistenceException 底层 channel 关闭失败时抛出并保留原因
     */
    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new InternalCatalogPersistenceException("close internal catalog failed: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 顺序扫描 header 已发布区域，并把物理 frame 还原为完整批次。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从固定 data 起点开始，以 sequence 1 和空 pending 批次建立期望状态。</li>
     *     <li>逐帧校验长度/CRC 后解析公共 body header；每个 frame 必须属于当前连续 sequence。</li>
     *     <li>data frame 按零起始 ordinal 追加记录并更新摘要；commit frame 必须精确匹配记录数和摘要，
     *     才把 pending 发布到结果并推进 sequence。</li>
     *     <li>扫描结束必须恰好落在 committed length，且不存在未封口 data frame，恢复出的下一
     *     sequence 必须与 header 一致；任一不符均视为已发布区域损坏。</li>
     * </ol>
     *
     * @param channel 待读取的 catalog channel
     * @param header 已通过 slot CRC 与基本字段校验的最新 header
     * @return 按物理提交顺序恢复的完整批次
     * @throws IOException positional read 失败或未取得进展时抛出，由打开入口包装为持久化异常
     * @throws InternalCatalogCorruptionException frame 或批次不满足 committed 格式时抛出
     */
    private static List<CatalogBatch> scanCommitted(FileChannel channel, Header header) throws IOException {
        // 1. header 的 next sequence 必须由从 1 开始的连续物理批次推导出来。
        List<CatalogBatch> result = new ArrayList<>();
        long position = DATA_START;
        long expectedSequence = 1;
        List<CatalogRecord> pending = new ArrayList<>();
        MessageDigest digest = sha256();
        while (position < header.committedLength) {
            // 2. 先完成 frame 边界与 CRC 校验，再解释其业务字段，防止损坏长度驱动越界读取。
            Frame frame = readFrame(channel, position, header.committedLength);
            position = frame.endOffset;
            ByteBuffer body = ByteBuffer.wrap(frame.body).order(ByteOrder.BIG_ENDIAN);
            byte type = body.get();
            long sequence = body.getLong();
            int ordinal = body.getInt();
            int keyLength = body.getInt();
            int payloadLength = body.getInt();
            if (sequence != expectedSequence) {
                throw new InternalCatalogCorruptionException("catalog batch sequence gap: expected="
                        + expectedSequence + ", actual=" + sequence);
            }

            // 3. data 累积 pending 与摘要；只有合法 commit 才将整个 pending 批次加入恢复结果。
            if (type == DATA_FRAME) {
                if (ordinal != pending.size() || keyLength <= 0 || keyLength > MAX_KEY_BYTES
                        || payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES
                        || body.remaining() != keyLength + payloadLength) {
                    throw new InternalCatalogCorruptionException("invalid internal catalog data frame");
                }
                byte[] key = new byte[keyLength];
                byte[] payload = new byte[payloadLength];
                body.get(key);
                body.get(payload);
                digest.update(frame.body);
                pending.add(new CatalogRecord(key, payload));
            } else if (type == COMMIT_FRAME) {
                if (ordinal != pending.size() || keyLength != 0 || payloadLength != SHA256_BYTES
                        || body.remaining() != SHA256_BYTES) {
                    throw new InternalCatalogCorruptionException("invalid internal catalog commit frame");
                }
                byte[] expectedHash = new byte[SHA256_BYTES];
                body.get(expectedHash);
                if (!Arrays.equals(expectedHash, digest.digest()) || pending.isEmpty()) {
                    throw new InternalCatalogCorruptionException("internal catalog batch hash mismatch");
                }
                result.add(new CatalogBatch(sequence, List.copyOf(pending)));
                pending.clear();
                digest = sha256();
                expectedSequence++;
            } else {
                throw new InternalCatalogCorruptionException("unknown internal catalog frame type: " + type);
            }
        }

        // 4. committed 边界、批次封口和 header 的 next sequence 必须相互证明，禁止容忍半批次。
        if (position != header.committedLength || !pending.isEmpty()
                || expectedSequence != header.nextBatchSequence) {
            throw new InternalCatalogCorruptionException("internal catalog header/batch boundary mismatch");
        }
        return result;
    }

    /**
     * 把一条 catalog 记录编码为 data frame body；frame 长度和 CRC 由外层写入。
     *
     * @param sequence 当前批次的持久 sequence
     * @param ordinal 记录在批次内从零开始的顺序号
     * @param record 已通过 v1 长度上限校验的记录
     * @return BIG_ENDIAN 的 data body 字节
     */
    private static byte[] dataBody(long sequence, int ordinal, CatalogRecord record) {
        byte[] key = record.key();
        byte[] payload = record.payload();
        ByteBuffer body = ByteBuffer.allocate(1 + Long.BYTES + Integer.BYTES * 3 + key.length + payload.length)
                .order(ByteOrder.BIG_ENDIAN);
        body.put(DATA_FRAME).putLong(sequence).putInt(ordinal).putInt(key.length).putInt(payload.length)
                .put(key).put(payload);
        return body.array();
    }

    /**
     * 编码批次 commit frame body，用记录数和摘要封闭此前 data frame。
     *
     * @param sequence 被提交批次的持久 sequence
     * @param count 批次内 data frame 数量
     * @param hash 按 data body 物理顺序计算的 SHA-256
     * @return BIG_ENDIAN 的 commit body 字节
     */
    private static byte[] commitBody(long sequence, int count, byte[] hash) {
        ByteBuffer body = ByteBuffer.allocate(1 + Long.BYTES + Integer.BYTES * 3 + hash.length)
                .order(ByteOrder.BIG_ENDIAN);
        body.put(COMMIT_FRAME).putLong(sequence).putInt(count).putInt(0).putInt(hash.length).put(hash);
        return body.array();
    }

    /**
     * 在指定偏移完整写入一个带长度和 CRC32C 前缀的 frame。
     *
     * @param channel catalog channel
     * @param position frame 起始字节偏移
     * @param body 已编码的 data 或 commit body
     * @return frame 末尾的 exclusive 偏移，可直接作为下一 frame 起点
     * @throws IOException positional write 失败或未取得进展时抛出
     */
    private static long writeFrame(FileChannel channel, long position, byte[] body) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(FRAME_PREFIX_BYTES + body.length).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(body.length).putInt(crc(body)).put(body).flip();
        writeFully(channel, frame, position);
        return position + frame.capacity();
    }

    /**
     * 从 committed 区域读取并校验一个完整 frame。
     *
     * <p>先确认固定前缀位于 committed 边界内，再用 v1 最大 body 大小约束长度，最后读取 body
     * 并验证 CRC32C；只有全部通过才向扫描器返回字节和下一偏移。</p>
     *
     * @param channel catalog channel
     * @param position frame 起始偏移
     * @param committedLength 最新 header 发布的 exclusive 文件边界
     * @return 已验证 frame body 及其 exclusive 结束偏移
     * @throws IOException positional read 失败或未取得进展时抛出
     * @throws InternalCatalogCorruptionException 前缀/body 越过提交边界、长度非法或 CRC 不匹配时抛出
     */
    private static Frame readFrame(FileChannel channel, long position, long committedLength) throws IOException {
        if (position + FRAME_PREFIX_BYTES > committedLength) {
            throw new InternalCatalogCorruptionException("truncated internal catalog frame prefix");
        }
        ByteBuffer prefix = ByteBuffer.allocate(FRAME_PREFIX_BYTES).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, prefix, position);
        prefix.flip();
        int length = prefix.getInt();
        int expectedCrc = prefix.getInt();
        int maxFrameBody = 1 + Long.BYTES + Integer.BYTES * 3 + MAX_KEY_BYTES + MAX_PAYLOAD_BYTES;
        if (length < 1 + Long.BYTES + Integer.BYTES * 3 || length > maxFrameBody
                || position + FRAME_PREFIX_BYTES + length > committedLength) {
            throw new InternalCatalogCorruptionException("invalid internal catalog frame length: " + length);
        }
        ByteBuffer body = ByteBuffer.allocate(length);
        readFully(channel, body, position + FRAME_PREFIX_BYTES);
        if (crc(body.array()) != expectedCrc) {
            throw new InternalCatalogCorruptionException("internal catalog frame CRC mismatch");
        }
        return new Frame(body.array(), position + FRAME_PREFIX_BYTES + length);
    }

    /**
     * 把 header 写入 generation 对应的交替 slot。
     *
     * <p>slot 内未使用区域保持零值，CRC 字段位于末尾并覆盖此前全部 4092 字节。该方法只执行写入，
     * durability 由调用方随后执行的 {@link FileChannel#force(boolean)} 建立。</p>
     *
     * @param channel catalog channel
     * @param header 待发布的 generation、committed 边界与下一 batch sequence
     * @throws IOException slot 的 positional write 失败时抛出
     */
    private static void writeHeader(FileChannel channel, Header header) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(HEADER_MAGIC).putInt(FORMAT_VERSION).putLong(header.generation)
                .putLong(header.committedLength).putLong(header.nextBatchSequence);
        buffer.position(HEADER_CRC_OFFSET);
        buffer.putInt(crc(buffer.array(), HEADER_CRC_OFFSET));
        buffer.flip();
        writeFully(channel, buffer, Math.floorMod(header.generation, 2) * (long) HEADER_SLOT_BYTES);
    }

    /**
     * 尝试从指定 slot 解码一个结构有效的 v1 header。
     *
     * <p>slot 不完整、CRC/魔数/版本/基本范围非法或读取异常均返回空，交由上层与另一槽共同裁决；
     * 本方法不校验 committed length 是否超过物理 EOF，也不扫描 frame。</p>
     *
     * @param channel catalog channel
     * @param slot header 槽号，只应为 0 或 1
     * @return 有效 header；槽不可用时返回 {@link Optional#empty()}
     */
    private static Optional<Header> decodeHeader(FileChannel channel, int slot) {
        try {
            if (channel.size() < (slot + 1L) * HEADER_SLOT_BYTES) {
                return Optional.empty();
            }
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, buffer, slot * (long) HEADER_SLOT_BYTES);
            byte[] bytes = buffer.array();
            if (ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(HEADER_CRC_OFFSET)
                    != crc(bytes, HEADER_CRC_OFFSET)) {
                return Optional.empty();
            }
            buffer.flip();
            if (buffer.getLong() != HEADER_MAGIC || buffer.getInt() != FORMAT_VERSION) {
                return Optional.empty();
            }
            Header header = new Header(buffer.getLong(), buffer.getLong(), buffer.getLong());
            if (header.generation <= 0 || header.committedLength < DATA_START || header.nextBatchSequence <= 0) {
                return Optional.empty();
            }
            return Optional.of(header);
        } catch (IOException | RuntimeException ignored) {
            return Optional.empty();
        }
    }

    /**
     * 校验一个待追加批次满足 catalog v1 的列表和字段大小边界。
     *
     * @param records 待追加记录；必须非空且不含空元素
     * @throws DatabaseValidationException 批次为空、含空记录或 key/payload 超过格式上限时抛出
     */
    private static void validateRecords(List<CatalogRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new DatabaseValidationException("internal catalog batch must not be null or empty");
        }
        for (CatalogRecord record : records) {
            if (record == null || record.key().length > MAX_KEY_BYTES || record.payload().length > MAX_PAYLOAD_BYTES) {
                throw new DatabaseValidationException("internal catalog record exceeds key/payload bounds");
            }
        }
    }

    /**
     * 在文件 IO 前拒绝空 catalog 路径。
     *
     * @param path 待打开或创建的路径
     * @throws DatabaseValidationException path 为空时抛出
     */
    private static void validatePath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("internal catalog path must not be null");
        }
    }

    /**
     * 创建新的 SHA-256 摘要器；每个待扫描或追加批次使用独立实例。
     *
     * @return 清空状态的 SHA-256 摘要器
     * @throws InternalCatalogCorruptionException JVM 缺少标准 SHA-256 实现、无法验证持久批次时抛出
     */
    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new InternalCatalogCorruptionException("JVM does not provide SHA-256", e);
        }
    }

    /**
     * 计算完整字节数组的 CRC32C。
     *
     * @param bytes 待校验 frame body
     * @return CRC32C 的低 32 位持久值
     */
    private static int crc(byte[] bytes) {
        return crc(bytes, bytes.length);
    }

    /**
     * 计算字节数组前缀的 CRC32C。
     *
     * @param bytes 包含待校验前缀的数组
     * @param length 从索引 0 开始纳入校验的字节数
     * @return CRC32C 的低 32 位持久值
     */
    private static int crc(byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    /**
     * 使用 positional write 消耗完整 buffer，拒绝无进展写入。
     *
     * @param channel 目标 channel
     * @param buffer 从当前位置写到 limit 的数据；成功后不再有 remaining
     * @param offset 首字节物理偏移
     * @throws IOException channel 写失败或返回非正进展时抛出
     */
    private static void writeFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written <= 0) {
                throw new IOException("internal catalog write made no progress");
            }
            position += written;
        }
    }

    /**
     * 使用 positional read 填满 buffer，拒绝 EOF 和无进展读取。
     *
     * @param channel 来源 channel
     * @param buffer 从当前位置填充到 limit 的目标 buffer
     * @param offset 首字节物理偏移
     * @throws IOException 到达 EOF、读取无进展或 channel 读取失败时抛出
     */
    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in internal catalog");
            }
            if (read == 0) {
                throw new IOException("internal catalog read made no progress");
            }
            position += read;
        }
    }

    /**
     * 一个 header slot 解码后的不可变状态。
     *
     * @param generation 双槽发布代次；值越大表示越新的成功发布
     * @param committedLength 已由该 generation 发布的 exclusive 文件字节边界
     * @param nextBatchSequence 下一次 append 必须使用的正数 sequence
     */
    private record Header(long generation, long committedLength, long nextBatchSequence) {
    }

    /**
     * 完成长度与 CRC 校验后的物理 frame。
     *
     * @param body 不含长度/CRC 前缀的原始 body
     * @param endOffset frame 的 exclusive 文件结束偏移
     */
    private record Frame(byte[] body, long endOffset) {
    }
}
