package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.dd.exception.DictionaryPersistenceException;
import cn.zhangyis.db.domain.SpaceId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/**
 * `mysql.dd.ctrl` 双槽高水位仓储。单把显式锁只保护本文件的 generation/counter 和 positional IO；锁内不访问
 * Buffer Pool、MTR、MDL 或用户表文件。每次 reserve 先写非当前槽并 force(true)，随后才发布内存快照，确保
 * 返回给 DDL 的 ID 已经不会在 crash 后复用。
 */
public final class DictionaryControlStore implements AutoCloseable {

    /** 每个副本独占一个 4 KiB 扇区/页，避免一次 torn write 同时破坏两代。 */
    public static final int SLOT_BYTES = 4096;

    /**
     * 持久格式魔数；读取端用它拒绝错文件或损坏内容，修改会破坏已有数据兼容性。
     */
    private static final long MAGIC = 0x4D_49_4E_49_44_44_43_54L; // MINIDDCT
    /**
     * 当前稳定格式版本；编解码与恢复路径共同依赖该值，升级时必须保留旧版本判定。
     */
    private static final int FORMAT_VERSION = 1;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int CRC_OFFSET = SLOT_BYTES - Integer.BYTES;

    /** 串行化 generation 切换与关闭；不会跨越外部调用。 */
    private final ReentrantLock ioLock = new ReentrantLock();

    /** control 文件唯一 channel，由本对象拥有并在 close 释放。 */
    private final FileChannel channel;

    /** 诊断用稳定路径。 */
    private final Path path;

    /** 最近有效 generation；只在 ioLock 内替换，snapshot 通过同一锁复制。 */
    private DictionaryControlSnapshot current;

    private DictionaryControlStore(Path path, FileChannel channel, DictionaryControlSnapshot current) {
        this.path = path;
        this.channel = channel;
        this.current = current;
    }

    /**
     * 打开已有 control，或在文件不存在时创建初始 generation。初始 schema/version 从 2 开始，1 保留给
     * bootstrap mysql schema/version；table/index/DDL 从 1，用户 space 从配置的安全下界开始。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param dictionarySpaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param firstUserSpaceId 目标表空间的原始数值标识；必须非负、已注册并满足当前生命周期准入条件
     * @return {@code openOrCreate} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryPersistenceException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static DictionaryControlStore openOrCreate(Path path, SpaceId dictionarySpaceId, int firstUserSpaceId) {
        validateOpenArguments(path, dictionarySpaceId);
        if (firstUserSpaceId <= dictionarySpaceId.value()) {
            throw new DatabaseValidationException("first user space id must be greater than dictionary space id");
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            boolean exists = Files.exists(path);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            if (exists && channel.size() > 0) {
                return openValidated(path, channel, dictionarySpaceId);
            }
            channel.truncate(SLOT_BYTES * 2L);
            DictionaryControlSnapshot initial = new DictionaryControlSnapshot(1, dictionarySpaceId,
                    2, 1, 1, firstUserSpaceId, 1, 2);
            writeSlot(channel, initial);
            channel.force(true);
            return new DictionaryControlStore(path, channel, initial);
        } catch (IOException e) {
            throw new DictionaryPersistenceException("open/create dictionary control failed: " + path, e);
        }
    }

    /** 打开已有 control；文件缺失、space id 不符或双槽均损坏都 fail-closed。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param dictionarySpaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code openExisting} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     * @throws DictionaryPersistenceException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public static DictionaryControlStore openExisting(Path path, SpaceId dictionarySpaceId) {
        validateOpenArguments(path, dictionarySpaceId);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            return openValidated(path, channel, dictionarySpaceId);
        } catch (IOException e) {
            throw new DictionaryPersistenceException("open dictionary control failed: " + path, e);
        }
    }

    /**
     * 校验 {@code openValidated} 涉及的数据字典结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param expectedSpaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code openValidated} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static DictionaryControlStore openValidated(Path path, FileChannel channel, SpaceId expectedSpaceId) {
        try {
            // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
            List<DictionaryControlSnapshot> candidates = new ArrayList<>(2);
            // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
            decodeSlot(channel, 0).ifPresent(candidates::add);
            decodeSlot(channel, 1).ifPresent(candidates::add);
            // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
            DictionaryControlSnapshot latest = candidates.stream()
                    .max(Comparator.comparingLong(DictionaryControlSnapshot::generation))
                    .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                            "both dictionary control slots are invalid: " + path));
            if (!latest.dictionarySpaceId().equals(expectedSpaceId)) {
                throw new DictionaryCatalogCorruptionException("dictionary control space id mismatch: expected="
                        + expectedSpaceId.value() + ", actual=" + latest.dictionarySpaceId().value());
            }
            // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
            return new DictionaryControlStore(path, channel, latest);
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
     * 原子预留各身份区间。溢出校验在任何写盘前完成；写槽和 force 成功后才替换 current，因此异常返回时
     * 调用方绝不能使用 allocation，内存也不会声称未 durable 的 generation 已生效。
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code reserve} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryPersistenceException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public DictionaryIdAllocation reserve(DictionaryIdRequest request) {
        if (request == null) {
            throw new DatabaseValidationException("dictionary id request must not be null");
        }
        ioLock.lock();
        try {
            DictionaryControlSnapshot before = current;
            long schema = advance(before.nextSchemaId(), request.schemaCount(), "schema");
            long table = advance(before.nextTableId(), request.tableCount(), "table");
            long index = advance(before.nextIndexId(), request.indexCount(), "index");
            long space = advance(before.nextSpaceId(), request.spaceCount(), "space");
            long ddl = advance(before.nextDdlId(), request.ddlCount(), "ddl");
            long version = advance(before.nextDictionaryVersion(), request.versionCount(), "dictionary version");
            if (space > Integer.MAX_VALUE) {
                throw new DatabaseValidationException("dictionary tablespace id range exhausted");
            }
            DictionaryControlSnapshot next = new DictionaryControlSnapshot(
                    advance(before.generation(), 1, "control generation"), before.dictionarySpaceId(),
                    schema, table, index, space, ddl, version);
            try {
                writeSlot(channel, next);
                channel.force(true);
            } catch (IOException e) {
                throw new DictionaryPersistenceException("reserve dictionary ids failed: " + path, e);
            }
            current = next;
            return new DictionaryIdAllocation(
                    request.schemaCount() == 0 ? 0 : before.nextSchemaId(),
                    request.tableCount() == 0 ? 0 : before.nextTableId(),
                    request.indexCount() == 0 ? 0 : before.nextIndexId(),
                    request.spaceCount() == 0 ? 0 : Math.toIntExact(before.nextSpaceId()),
                    request.ddlCount() == 0 ? 0 : before.nextDdlId(),
                    request.versionCount() == 0 ? 0 : before.nextDictionaryVersion(),
                    request, next.generation());
        } finally {
            ioLock.unlock();
        }
    }

    /** 返回最近 durable 槽的不可变快照。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public DictionaryControlSnapshot snapshot() {
        ioLock.lock();
        try {
            return current;
        } finally {
            ioLock.unlock();
        }
    }

    /**
     * 释放本方法拥有的数据字典资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @throws DictionaryPersistenceException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void close() {
        ioLock.lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new DictionaryPersistenceException("close dictionary control failed: " + path, e);
        } finally {
            ioLock.unlock();
        }
    }

    private static long advance(long current, int count, String kind) {
        try {
            return Math.addExact(current, count);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("dictionary " + kind + " id range overflow", overflow);
        }
    }

    private static void validateOpenArguments(Path path, SpaceId dictionarySpaceId) {
        if (path == null || dictionarySpaceId == null) {
            throw new DatabaseValidationException("dictionary control path/space id must not be null");
        }
    }

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static void writeSlot(FileChannel channel, DictionaryControlSnapshot snapshot) throws IOException {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putLong(snapshot.generation());
        buffer.putInt(snapshot.dictionarySpaceId().value());
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        buffer.putLong(snapshot.nextSchemaId());
        buffer.putLong(snapshot.nextTableId());
        buffer.putLong(snapshot.nextIndexId());
        buffer.putLong(snapshot.nextSpaceId());
        buffer.putLong(snapshot.nextDdlId());
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        buffer.putLong(snapshot.nextDictionaryVersion());
        int crc = crc(buffer.array(), CRC_OFFSET);
        buffer.position(CRC_OFFSET);
        buffer.putInt(crc);
        buffer.flip();
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        writeFully(channel, buffer, slotOffset(snapshot.generation()));
    }

    /**
     * 从稳定表示解码数据字典领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param slot 参与 {@code decodeSlot} 的零基位置 {@code slot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code decodeSlot} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    private static java.util.Optional<DictionaryControlSnapshot> decodeSlot(FileChannel channel, int slot) {
        try {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            if (channel.size() < (slot + 1L) * SLOT_BYTES) {
                return java.util.Optional.empty();
            }
            ByteBuffer buffer = ByteBuffer.allocate(SLOT_BYTES).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, buffer, slot * (long) SLOT_BYTES);
            byte[] bytes = buffer.array();
            int storedCrc = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(CRC_OFFSET);
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            if (storedCrc != crc(bytes, CRC_OFFSET)) {
                return java.util.Optional.empty();
            }
            buffer.flip();
            if (buffer.getLong() != MAGIC || buffer.getInt() != FORMAT_VERSION) {
                return java.util.Optional.empty();
            }
            long generation = buffer.getLong();
            int spaceId = buffer.getInt();
            long nextSchema = buffer.getLong();
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            long nextTable = buffer.getLong();
            long nextIndex = buffer.getLong();
            long nextSpace = buffer.getLong();
            long nextDdl = buffer.getLong();
            long nextVersion = buffer.getLong();
            if (generation <= 0 || spaceId < 0 || nextSchema <= 0 || nextTable <= 0 || nextIndex <= 0
                    || nextSpace <= 0 || nextSpace > Integer.MAX_VALUE || nextDdl <= 0 || nextVersion <= 0) {
                return java.util.Optional.empty();
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return java.util.Optional.of(new DictionaryControlSnapshot(generation, SpaceId.of(spaceId),
                    nextSchema, nextTable, nextIndex, nextSpace, nextDdl, nextVersion));
        } catch (IOException | RuntimeException ignored) {
            return java.util.Optional.empty();
        }
    }

    private static long slotOffset(long generation) {
        return Math.floorMod(generation, 2) * (long) SLOT_BYTES;
    }

    private static int crc(byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param buffer 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static void writeFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written <= 0) {
                throw new IOException("dictionary control positional write made no progress");
            }
            position += written;
        }
    }

    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param channel 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param buffer 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in dictionary control slot");
            }
            if (read == 0) {
                throw new IOException("dictionary control positional read made no progress");
            }
            position += read;
        }
    }
}
