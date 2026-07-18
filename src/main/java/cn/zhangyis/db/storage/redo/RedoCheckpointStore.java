package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Redo checkpoint control v2 双槽仓储。两个 52B slot 分别位于独立 4 KiB 文件页，避免一次 torn sector/page
 * write 同时破坏两份 label；写槽 force 成功后才切换 nextSlot/generation。
 *
 * <p>slot 同时绑定 redo data format version。已知 control v1 明确 fail-closed，不能退化为 checkpoint 0 后
 * 与 LogBlock v1 混用。READ_ONLY_VALIDATE 使用只读 channel，不创建、预分配或写槽。
 */
public final class RedoCheckpointStore implements AutoCloseable {

    /** slot magic：ASCII "RCP2"，沿用稳定文件身份。 */
    static final int MAGIC = 0x52435032;
    /** control v2：增加 redo format/generation，并物理分隔双槽。 */
    private static final int CONTROL_FORMAT_VERSION = 2;
    /** magic/controlVersion/redoVersion/reserved/checkpoint/current/time/generation/crc。
     *
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    static final int SLOT_BYTES = 52;
    /** 双槽起始偏移跨度。 */
    static final int SLOT_STRIDE_BYTES = 4 * 1024;
    /** 固定双槽文件长度。 */
    private static final long FILE_BYTES = 2L * SLOT_STRIDE_BYTES;
    /** checksum 覆盖 checksum 字段之前的全部 slot bytes。 */
    private static final int CHECKSUM_BYTES = SLOT_BYTES - Integer.BYTES;

    /** control 路径，用于格式/IO 诊断。 */
    private final Path path;
    /** positional IO channel；只读实例没有 WRITE capability。 */
    private final FileChannel channel;
    /** 是否允许预分配和写槽。 */
    private final boolean writable;
    /** 保护 slot 选择、读写、force 与关闭。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 下一次覆盖槽编号，由最新有效 generation 恢复。 */
    private int nextSlot;
    /** 下一次写入 generation；只在 force 成功后递增。 */
    private long nextGeneration;

    /**
     * 创建 {@code RedoCheckpointStore}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param writable 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private RedoCheckpointStore(Path path, FileChannel channel, boolean writable) throws IOException {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        this.path = path;
        this.channel = channel;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.writable = writable;
        rejectLegacyControl();
        if (channel.size() > FILE_BYTES) {
            throw new RedoLogCorruptedException(
                    "redo checkpoint control exceeds fixed v2 size: " + channel.size() + " > " + FILE_BYTES);
        }
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        if (writable) {
            ensureFixedFileSize();
        }
        Optional<Slot> latest = latestSlot();
        nextSlot = latest.map(slot -> 1 - slot.slotIndex()).orElse(0);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        nextGeneration = latest.map(slot -> incrementGeneration(slot.generation())).orElse(1L);
    }

    /** 打开或创建 writable control v2；旧 v1 文件在任何预分配/写入前拒绝。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code open} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static RedoCheckpointStore open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("redo checkpoint control path must not be null");
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            return construct(path, channel, true);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open redo checkpoint control file: " + path, e);
        }
    }

    /** 只读打开 existing control v2；路径缺失时不创建文件。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code openReadOnly} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static RedoCheckpointStore openReadOnly(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("read-only redo checkpoint path must not be null");
        }
        try {
            return construct(path, FileChannel.open(path, StandardOpenOption.READ), false);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open read-only redo checkpoint control: " + path, e);
        }
    }

    private static RedoCheckpointStore construct(Path path, FileChannel channel, boolean writable) throws IOException {
        try {
            return new RedoCheckpointStore(path, channel, writable);
        } catch (RuntimeException | IOException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** 读取 checkpoint 最大、同 checkpoint 下 generation 最大的有效 label；双槽均无效时安全回到当前格式初始值。
     *
     * @return {@code readLatest} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public RedoCheckpointLabel readLatest() {
        ioLock.lock();
        try {
            return latestSlot().map(Slot::label).orElseGet(RedoCheckpointLabel::initial);
        } finally {
            ioLock.unlock();
        }
    }

    /** 写入并 force 一个当前 redo data 格式的 checkpoint label。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param label redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public void write(RedoCheckpointLabel label) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (label == null) {
            throw new DatabaseValidationException("redo checkpoint label must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        requireWritable();
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        if (label.redoFormatVersion() != RedoLogBlockCodec.FORMAT_VERSION) {
            throw new RedoLogFormatException("cannot persist checkpoint for redo format "
                    + label.redoFormatVersion() + "; current=" + RedoLogBlockCodec.FORMAT_VERSION);
        }
        ioLock.lock();
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        try {
            ByteBuffer encoded = encode(label, nextGeneration);
            long offset = slotOffset(nextSlot);
            writeFullyAt(offset, encoded);
            channel.force(true);
            nextSlot = 1 - nextSlot;
            nextGeneration = incrementGeneration(nextGeneration);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to write redo checkpoint label to " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 关闭 control channel；失败保留底层 cause。
     *
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to close redo checkpoint control file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private Optional<Slot> latestSlot() {
        Optional<Slot> first = readSlot(0);
        Optional<Slot> second = readSlot(1);
        if (first.isEmpty()) {
            return second;
        }
        if (second.isEmpty()) {
            return first;
        }
        return Optional.of(newer(first.get(), second.get()));
    }

    /**
     * 定位并读取Redo/WAL领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param slotIndex 参与 {@code readSlot} 的零基位置 {@code slotIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readSlot} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws RedoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private Optional<Slot> readSlot(int slotIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        long offset = slotOffset(slotIndex);
        try {
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, offset + buffer.position());
                if (read < 0 || read == 0) {
                    return Optional.empty();
                }
            }
            byte[] bytes = buffer.array();
            ByteBuffer view = ByteBuffer.wrap(bytes);
            int magic = view.getInt();
            if (magic != MAGIC) {
                return Optional.empty();
            }
            // version/format 等字段只有在整槽 CRC 通过后才可信。若先解释 version，单字节 torn write 会被
            // 错报为“不支持格式”，并阻止另一个有效槽接管恢复。
            if (view.getInt(CHECKSUM_BYTES) != checksum(bytes)) {
                return Optional.empty();
            }
            int controlVersion = view.getInt();
            if (controlVersion != CONTROL_FORMAT_VERSION) {
                throw new RedoLogFormatException(
                        "unsupported redo checkpoint control version " + controlVersion + ": " + path);
            }
            int redoFormatVersion = view.getInt();
            int reserved = view.getInt();
            if (reserved != 0) {
                throw new RedoLogCorruptedException("redo checkpoint reserved field is non-zero: " + path);
            }
            long generation = view.getLong(40);
            if (generation <= 0) {
                throw new RedoLogCorruptedException("redo checkpoint generation is invalid: " + generation);
            }
            RedoCheckpointLabel label = RedoCheckpointLabel.decoded(
                    Lsn.of(view.getLong(16)), Lsn.of(view.getLong(24)),
                    view.getLong(32), redoFormatVersion);
            return Optional.of(new Slot(slotIndex, label, generation));
        } catch (IOException e) {
            throw new RedoLogIoException("failed to read redo checkpoint slot from " + path, e);
        } catch (DatabaseValidationException e) {
            throw new RedoLogCorruptedException(
                    "redo checkpoint slot contains checksum-valid invalid fields: " + path, e);
        }
    }

    private static Slot newer(Slot first, Slot second) {
        int checkpointOrder = Long.compare(
                first.label().checkpointLsn().value(), second.label().checkpointLsn().value());
        if (checkpointOrder != 0) {
            return checkpointOrder > 0 ? first : second;
        }
        return first.generation() >= second.generation() ? first : second;
    }

    /**
     * 把调用方领域值编码为Redo/WAL的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param label redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param generation 参与 {@code encode} 的单调版本值 {@code generation}；必须非负，回退或与权威快照冲突时拒绝
     * @return {@code encode} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static ByteBuffer encode(RedoCheckpointLabel label, long generation) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(CONTROL_FORMAT_VERSION);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        buffer.putInt(label.redoFormatVersion());
        buffer.putInt(0);
        buffer.putLong(label.checkpointLsn().value());
        buffer.putLong(label.currentLsnAtCheckpoint().value());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        buffer.putLong(label.createdAtMillis());
        buffer.putLong(generation);
        buffer.putInt(checksum(buffer.array()));
        buffer.flip();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return buffer;
    }

    /** 在扩展文件前读取首 8B，确保已知 v1 不会被 v2 预分配覆盖。 */
    private void rejectLegacyControl() throws IOException {
        if (channel.size() < 8) {
            return;
        }
        ByteBuffer prefix = ByteBuffer.allocate(8);
        while (prefix.hasRemaining()) {
            int read = channel.read(prefix, prefix.position());
            if (read <= 0) {
                return;
            }
        }
        prefix.flip();
        if (prefix.getInt() == MAGIC && prefix.getInt() == 1) {
            throw new RedoLogFormatException("legacy redo checkpoint control v1 is not supported: " + path);
        }
    }

    /** 预分配两个独立 4 KiB 页；零 padding 不会形成合法 slot。 */
    private void ensureFixedFileSize() throws IOException {
        if (channel.size() >= FILE_BYTES) {
            return;
        }
        writeFullyAt(FILE_BYTES - 1, ByteBuffer.wrap(new byte[]{0}));
        channel.force(true);
    }

    /** positional write 必须持续前进；零进度不能在 control 临界区无限自旋。
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
                throw new RedoLogIoException("zero-progress write while updating redo checkpoint: " + path);
            }
            position += written;
        }
    }

    private void requireWritable() {
        if (!writable) {
            throw new DatabaseValidationException("read-only redo checkpoint store cannot write: " + path);
        }
    }

    private static long slotOffset(int slotIndex) {
        return (long) slotIndex * SLOT_STRIDE_BYTES;
    }

    private static int checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, CHECKSUM_BYTES);
        return (int) crc.getValue();
    }

    private static long incrementGeneration(long generation) {
        try {
            return Math.addExact(generation, 1L);
        } catch (ArithmeticException e) {
            throw new RedoLogCorruptedException("redo checkpoint generation overflow: " + generation, e);
        }
    }

    /**
     * 封装Redo/WAL中 {@code Slot} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param slotIndex 参与 {@code 构造} 的零基位置 {@code slotIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param label redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param generation 参与 {@code 构造} 的单调版本值 {@code generation}；必须非负，回退或与权威快照冲突时拒绝
     */
    private record Slot(int slotIndex, RedoCheckpointLabel label, long generation) {
    }
}
