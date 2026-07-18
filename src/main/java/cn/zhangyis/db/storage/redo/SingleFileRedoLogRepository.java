package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单文件 LogBlock v1 repository。每个 MTR batch 独占一个封口 block chain；扫描只确认完整 batch，末尾 torn
 * chain 的物理字节直到下一次 writable append 才被截断，因此 READ_ONLY_VALIDATE 不会改变恢复输入。
 *
 * <p>单把 {@link #ioLock} 串行 append/force/scan/close。锁内可能执行文件 IO，但不会获取 page latch、事务锁或
 * Buffer Pool 锁，依赖方向保持 redo repository 自包含。
 */
public final class SingleFileRedoLogRepository implements RedoLogFileRepository {

    /** redo 文件路径，用于格式和 IO 异常诊断。 */
    private final Path path;
    /** positional/append IO channel；只读实例不带 WRITE capability。 */
    private final FileChannel channel;
    /** 是否允许 truncate/append/force；READ_ONLY_VALIDATE 固定 false。 */
    private final boolean writable;
    /** 串行 channel 生命周期和恢复边界状态。 */
    private final ReentrantLock ioLock = new ReentrantLock();
    /** 最后一个完整 batch 之后的物理偏移；torn chain 从这里开始。由 ioLock 保护。 */
    private long validBytes;
    /** 下一次 append 使用的 blockNo；torn chain 的编号不会消耗。由 ioLock 保护。 */
    private long nextBlockNo;
    /** 最后完整 batch 的逻辑 end LSN。由 ioLock 保护。 */
    private long endLsn;

    /**
     * 创建 {@code SingleFileRedoLogRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param writable 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
     */
    private SingleFileRedoLogRepository(Path path, FileChannel channel, boolean writable) {
        this.path = path;
        this.channel = channel;
        this.writable = writable;
        loadState();
    }

    /** 打开或创建 writable 单文件 repository；已有内容必须是 LogBlock v1。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code open} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static SingleFileRedoLogRepository open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("redo log path must not be null");
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
            throw new RedoLogIoException("failed to open redo log file: " + path, e);
        }
    }

    /** 只读打开已存在单文件 repository；不创建父目录或缺失文件。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code openReadOnly} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static SingleFileRedoLogRepository openReadOnly(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("read-only redo log path must not be null");
        }
        try {
            return construct(path, FileChannel.open(path, StandardOpenOption.READ), false);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to open read-only redo log file: " + path, e);
        }
    }

    /** 构造失败时关闭刚打开的 channel，关闭异常作为 suppressed 保留而不覆盖格式根因。 */
    private static SingleFileRedoLogRepository construct(Path path, FileChannel channel, boolean writable) {
        try {
            return new SingleFileRedoLogRepository(path, channel, writable);
        } catch (RuntimeException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /**
     * 追加一个 LSN 连续的 batch。编码和逻辑边界校验发生在 truncate/write 前；若上次扫描存在 torn tail，
     * 只在本次真正写入时从 {@link #validBytes} 截断，避免新 redo 落到不可达损坏字节之后。
     * @param batch redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     * @throws RedoLogCapacityExceededException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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
                throw new RedoLogCorruptedException("single-file redo append LSN is discontinuous: expected="
                        + endLsn + ", actual=" + batch.range().start().value());
            }
            RedoLogBlockCodec.EncodedBatch encoded = RedoLogBlockCodec.encodeBatch(batch, nextBlockNo);
            if (validBytes > Integer.MAX_VALUE - encoded.byteLength()) {
                // 当前教学实现用单个 ByteBuffer 做 recovery snapshot；在跨过上限前拒绝，不能先写出一个
                // 本实现下次启动无法扫描的文件。默认 ring 不受该 standalone opt-out 简化限制。
                throw new RedoLogCapacityExceededException(
                        "single redo file would exceed LogBlock scanner limit: "
                                + (validBytes + encoded.byteLength()));
            }
            if (channel.size() != validBytes) {
                channel.truncate(validBytes);
            }
            ByteBuffer bytes = encoded.bytes();
            writeFullyAt(validBytes, bytes);
            validBytes += encoded.byteLength();
            nextBlockNo = encoded.nextBlockNo();
            endLsn = batch.range().end().value();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to append LogBlock batch to file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** force 只允许 writable 生命周期调用；read-only 绝不把扫描变成隐式写入。
     *
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void force() {
        requireWritable("force");
        ioLock.lock();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new RedoLogIoException("failed to force redo log file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 扫描全部完整 batch 并刷新内存续写边界；方法本身不 truncate 或改写文件。
     *
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    @Override
    public List<RedoLogBatch> readBatches() {
        ioLock.lock();
        try {
            return scanAndPublish().batches();
        } finally {
            ioLock.unlock();
        }
    }

    /** 当前 repository 的 LogBlock 持久格式版本。 */
    @Override
    public int formatVersion() {
        return RedoLogBlockCodec.FORMAT_VERSION;
    }

    /** 关闭 channel；失败保留底层 cause。
     *
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to close redo log file: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    /** 打开阶段扫描恢复 append 边界；任何旧格式/中段损坏在对象发布前失败。 */
    private void loadState() {
        ioLock.lock();
        try {
            scanAndPublish();
        } finally {
            ioLock.unlock();
        }
    }

    /** 读取文件快照并通过共享 scanner 校验；当前单文件实现显式限制单文件内容不超过 Java ByteBuffer 上限。 */
    private RedoLogBlockScanResult scanAndPublish() {
        try {
            long size = channel.size();
            if (size > Integer.MAX_VALUE) {
                throw new RedoLogCorruptedException(
                        "single redo file exceeds LogBlock v1 scanner limit: " + size);
            }
            ByteBuffer content = ByteBuffer.allocate((int) size);
            long position = 0;
            while (content.hasRemaining()) {
                int read = channel.read(content, position);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    throw new RedoLogIoException("zero-progress read while scanning redo file: " + path);
                }
                position += read;
            }
            content.flip();
            RedoLogBlockScanResult result = RedoLogBlockScanner.scan(
                    content, true, OptionalLong.of(0L), 0L);
            validBytes = result.validBytes();
            nextBlockNo = result.nextBlockNo();
            endLsn = result.endLsn().value();
            return result;
        } catch (IOException e) {
            throw new RedoLogIoException("failed to scan redo LogBlocks from " + path, e);
        }
    }

    private void requireWritable(String operation) {
        if (!writable) {
            throw new DatabaseValidationException(
                    "read-only redo log repository cannot " + operation + ": " + path);
        }
    }

    /** positional write 必须持续前进；零进度按 IO 失败处理，避免持 ioLock 无限自旋。
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
                throw new RedoLogIoException("zero-progress write while appending redo file: " + path);
            }
            position += written;
        }
    }
}
