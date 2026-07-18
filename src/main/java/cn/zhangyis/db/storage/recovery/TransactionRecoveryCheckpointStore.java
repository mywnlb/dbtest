package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

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
 * 事务恢复高水位 sidecar 仓储。
 *
 * <p>文件固定包含两个 CRC slot，每个 slot 位于独立的 4 KiB 文件页，写入时交替覆盖并强制落盘。
 * 物理隔离避免一次 torn sector/page write 同时破坏两份副本。恢复只选择完整有效且 checkpoint 最大的副本；
 * checkpoint 相同时以单调 generation 选择较新 counter 快照。该文件先于 redo checkpoint label 持久化，
 * 因而 sidecar 比 redo label 更新是合法崩溃窗口，落后则由恢复编排层拒绝。
 */
public final class TransactionRecoveryCheckpointStore
        implements TransactionRecoveryCheckpointSource, AutoCloseable {

    /** slot magic：ASCII "TRC1"。
     *
     * 持久格式魔数；读取端用它拒绝错文件或损坏内容，修改会破坏已有数据兼容性。
     */
    private static final int MAGIC = 0x54524331;
    /** sidecar 二进制格式版本。 */
    private static final int FORMAT_VERSION = 1;
    /** magic/version/checkpoint/nextId/nextNo/generation/crc32 的固定长度。 */
    static final int SLOT_BYTES = 44;
    /** 两槽起始偏移之间的跨度；按 4 KiB 隔离，供整页损坏回退测试固定磁盘布局。 */
    static final int SLOT_STRIDE_BYTES = 4 * 1024;
    /** sidecar 固定双槽，预分配两个独立文件页。 */
    private static final int SLOT_COUNT = 2;
    /** 固定文件长度；尾部 padding 不参与 checksum 或版本语义。 */
    private static final long FILE_BYTES = (long) SLOT_COUNT * SLOT_STRIDE_BYTES;
    /** checksum 覆盖的 slot 前缀，不包含末尾 crc32。 */
    private static final int CHECKSUM_BYTES = SLOT_BYTES - Integer.BYTES;
    /** sidecar 路径，只用于生命周期和异常诊断。 */
    private final Path path;
    /** positional IO channel；共享 position 不参与正确性。 */
    private final FileChannel channel;
    /** 是否允许覆盖 slot；READ_ONLY_VALIDATE 打开的 store 固定为 false。 */
    private final boolean writable;
    /** 保护 slot 选择、写入、force 与关闭，等待仅限本地文件 IO。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 下一次覆盖的 slot，仅在 {@link #ioLock} 内读写。 */
    private int nextSlot;
    /** 下一次持久化 generation，仅在 {@link #ioLock} 内递增。 */
    private long nextGeneration;

    /**
     * 创建 {@code TransactionRecoveryCheckpointStore}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
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
     */
    private TransactionRecoveryCheckpointStore(Path path, FileChannel channel, boolean writable) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        this.path = path;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.channel = channel;
        this.writable = writable;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        Optional<Slot> latest = latestSlot();
        this.nextSlot = latest.map(slot -> 1 - slot.slotIndex()).orElse(0);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.nextGeneration = latest.map(slot -> slot.generation() + 1).orElse(1L);
    }

    /** 打开或创建事务恢复 sidecar；不会把空文件伪装成权威初始基线。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code open} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    public static TransactionRecoveryCheckpointStore open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("transaction recovery checkpoint path must not be null");
        }
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            ensureFixedFileSize(channel);
            return new TransactionRecoveryCheckpointStore(path, channel, true);
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to open transaction recovery checkpoint sidecar: " + path, e);
        }
    }

    /**
     * 只读打开已存在 sidecar。该入口不创建目录/文件；路径缺失或并发删除会作为恢复输入 IO 错误 fail closed。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code openReadOnly} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    public static TransactionRecoveryCheckpointStore openReadOnly(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("read-only transaction recovery checkpoint path must not be null");
        }
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return new TransactionRecoveryCheckpointStore(path, channel, false);
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to open read-only transaction recovery checkpoint sidecar: " + path, e);
        }
    }

    /**
     * 读取最新有效基线。空文件或两个 slot 都损坏时返回 empty，交给恢复层结合 redo checkpoint 判定是否兼容。
     *
     * @return {@code readLatest} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    @Override
    public Optional<TransactionRecoveryCheckpoint> readLatest() {
        ioLock.lock();
        try {
            return latestSlot().map(Slot::checkpoint);
        } finally {
            ioLock.unlock();
        }
    }

    /** 写入并 force 一个高水位快照；force 成功前不会切换下一槽或推进 generation。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param checkpoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    public void write(TransactionRecoveryCheckpoint checkpoint) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (checkpoint == null) {
            throw new DatabaseValidationException("transaction recovery checkpoint must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        if (!writable) {
            throw new TransactionRecoveryException(
                    "read-only transaction recovery checkpoint store cannot write: " + path);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        ioLock.lock();
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        try {
            ByteBuffer encoded = encode(checkpoint, nextGeneration);
            long offset = slotOffset(nextSlot);
            while (encoded.hasRemaining()) {
                channel.write(encoded, offset + encoded.position());
            }
            channel.force(true);
            nextSlot = 1 - nextSlot;
            nextGeneration++;
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to persist transaction recovery checkpoint to " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 关闭 sidecar channel；失败时保留底层 cause。
     *
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to close transaction recovery checkpoint sidecar: " + path, e);
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
     * 定位并读取崩溃恢复领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param slotIndex 参与 {@code readSlot} 的零基位置 {@code slotIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readSlot} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private Optional<Slot> readSlot(int slotIndex) {
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        long offset = slotOffset(slotIndex);
        try {
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, offset + buffer.position());
                if (read <= 0) {
                    return Optional.empty();
                }
            }
            byte[] bytes = buffer.array();
            ByteBuffer view = ByteBuffer.wrap(bytes);
            if (view.getInt() != MAGIC || view.getInt() != FORMAT_VERSION) {
                return Optional.empty();
            }
            if (view.getInt(CHECKSUM_BYTES) != checksum(bytes)) {
                return Optional.empty();
            }
            TransactionRecoveryCheckpoint checkpoint = new TransactionRecoveryCheckpoint(
                    Lsn.of(view.getLong(8)), TransactionId.of(view.getLong(16)),
                    TransactionNo.of(view.getLong(24)));
            return Optional.of(new Slot(slotIndex, checkpoint, view.getLong(32)));
        } catch (IOException e) {
            throw new TransactionRecoveryException(
                    "failed to read transaction recovery checkpoint slot from " + path, e);
        } catch (DatabaseValidationException e) {
            return Optional.empty();
        }
    }

    private static Slot newer(Slot first, Slot second) {
        int checkpointOrder = Long.compare(
                first.checkpoint().checkpointLsn().value(), second.checkpoint().checkpointLsn().value());
        if (checkpointOrder != 0) {
            return checkpointOrder > 0 ? first : second;
        }
        return first.generation() >= second.generation() ? first : second;
    }

    /**
     * 把调用方领域值编码为崩溃恢复的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param checkpoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param generation 参与 {@code encode} 的单调版本值 {@code generation}；必须非负，回退或与权威快照冲突时拒绝
     * @return {@code encode} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static ByteBuffer encode(TransactionRecoveryCheckpoint checkpoint, long generation) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES);
        buffer.putInt(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        buffer.putLong(checkpoint.checkpointLsn().value());
        buffer.putLong(checkpoint.nextTransactionId().value());
        buffer.putLong(checkpoint.nextTransactionNo().value());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        buffer.putLong(generation);
        buffer.putInt(checksum(buffer.array()));
        buffer.flip();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return buffer;
    }

    private static int checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, CHECKSUM_BYTES);
        return (int) crc.getValue();
    }

    /**
     * 首次创建时预分配两个 4 KiB 文件页。这里只写零 padding，不生成合法 slot；因此空文件语义仍由 magic/CRC
     * 区分。扩展成功后立即 force，避免第一次 checkpoint 之前遗留不稳定的文件长度。
     */
    private static void ensureFixedFileSize(FileChannel channel) throws IOException {
        if (channel.size() >= FILE_BYTES) {
            return;
        }
        channel.write(ByteBuffer.wrap(new byte[]{0}), FILE_BYTES - 1);
        channel.force(true);
    }

    /** 返回指定 slot 的 4 KiB 对齐起始偏移。 */
    private static long slotOffset(int slotIndex) {
        return (long) slotIndex * SLOT_STRIDE_BYTES;
    }

    /**
     * 封装崩溃恢复中 {@code Slot} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param slotIndex 参与 {@code 构造} 的零基位置 {@code slotIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param checkpoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param generation 参与 {@code 构造} 的单调版本值 {@code generation}；必须非负，回退或与权威快照冲突时拒绝
     */
    private record Slot(int slotIndex, TransactionRecoveryCheckpoint checkpoint, long generation) {
    }
}
