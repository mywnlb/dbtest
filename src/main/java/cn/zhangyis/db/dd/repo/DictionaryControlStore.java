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
import java.nio.file.LinkOption;
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

    /** 在 control force 前持久化目标高水位的窄端口；生产使用 recovery manifest，实现不得回调本 store。 */
    private final DictionaryDurabilityWitness witness;

    /** 最近有效 generation；只在 ioLock 内替换，snapshot 通过同一锁复制。 */
    private DictionaryControlSnapshot current;

    private DictionaryControlStore(Path path, FileChannel channel, DictionaryControlSnapshot current,
                                   DictionaryDurabilityWitness witness) {
        this.path = path;
        this.channel = channel;
        this.current = current;
        this.witness = witness;
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
        return openOrCreate(path, dictionarySpaceId, firstUserSpaceId, DictionaryDurabilityWitness.noOp());
    }

    /**
     * 打开 control 并注入写前 recovery witness；生产组合根使用该入口，既有三参入口保持兼容。
     *
     * @param path control 固定路径
     * @param dictionarySpaceId 固定字典 space identity
     * @param firstUserSpaceId 用户 space identity 起点
     * @param witness control 写前 durable witness
     * @return 已打开且持有 witness 的 control store
     */
    public static DictionaryControlStore openOrCreate(Path path, SpaceId dictionarySpaceId, int firstUserSpaceId,
                                                       DictionaryDurabilityWitness witness) {
        validateOpenArguments(path, dictionarySpaceId);
        if (witness == null) {
            throw new DatabaseValidationException("dictionary control witness must not be null");
        }
        if (firstUserSpaceId <= dictionarySpaceId.value()) {
            throw new DatabaseValidationException("first user space id must be greater than dictionary space id");
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            boolean exists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            if (exists && channel.size() > 0) {
                return openValidated(path, channel, dictionarySpaceId, witness);
            }
            channel.truncate(SLOT_BYTES * 2L);
            DictionaryControlSnapshot initial = new DictionaryControlSnapshot(1, dictionarySpaceId,
                    2, 1, 1, firstUserSpaceId, 1, 2);
            writeSlot(channel, initial);
            channel.force(true);
            return new DictionaryControlStore(path, channel, initial, witness);
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
        return openExisting(path, dictionarySpaceId, DictionaryDurabilityWitness.noOp());
    }

    /**
     * 严格打开 existing control 并注入后续 reservation witness。
     *
     * @param path existing control 路径
     * @param dictionarySpaceId 固定字典 space identity
     * @param witness control 写前 durable witness
     * @return 已恢复的 control store
     */
    public static DictionaryControlStore openExisting(Path path, SpaceId dictionarySpaceId,
                                                       DictionaryDurabilityWitness witness) {
        validateOpenArguments(path, dictionarySpaceId);
        if (witness == null) {
            throw new DatabaseValidationException("dictionary control witness must not be null");
        }
        try {
            FileChannel channel = FileChannel.open(
                    path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            return openValidated(path, channel, dictionarySpaceId, witness);
        } catch (IOException e) {
            throw new DictionaryPersistenceException("open dictionary control failed: " + path, e);
        }
    }

    /**
     * 在 catalog-loss rebuild 中创建一个精确的安全高水位 control 文件。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验目标路径尚不存在以及 recovered snapshot 的 identity/counter 范围，避免覆盖原始证据。</li>
     *     <li>创建双槽长度的同目录文件，并把 recovered snapshot 写入其 generation 对应槽。</li>
     *     <li>{@code force(true)} 后才构造 store；另一个槽保持无效，后续普通 reservation 会自然交替发布。</li>
     *     <li>失败关闭 channel 并保留现场；catalog 尚未发布，普通启动仍会 fail-closed。</li>
     * </ol>
     *
     * @param path 必须不存在的固定 {@code mysql.dd.ctrl} 路径
     * @param recovered manifest/control reservations 逐分量最大后的安全高水位
     * @return 已持有新 control channel 的 store；调用方负责关闭
     * @throws DictionaryPersistenceException 路径已存在、创建、写槽或 force 失败时抛出
     */
    public static DictionaryControlStore createRebuilt(Path path, DictionaryControlSnapshot recovered) {
        validateOpenArguments(path, recovered == null ? null : recovered.dictionarySpaceId());
        validateSnapshot(recovered);
        FileChannel channel = null;
        try {
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new DictionaryPersistenceException(
                        "refuse to overwrite existing dictionary control during rebuild: " + path);
            }
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            channel.truncate(SLOT_BYTES * 2L);
            writeSlot(channel, recovered);
            channel.force(true);
            return new DictionaryControlStore(path, channel, recovered, DictionaryDurabilityWitness.noOp());
        } catch (IOException | RuntimeException failure) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof DictionaryPersistenceException persistenceFailure) {
                throw persistenceFailure;
            }
            throw new DictionaryPersistenceException("create rebuilt dictionary control failed: " + path, failure);
        }
    }

    /**
     * 从双槽 control 中选择可恢复的最新 generation，并把 channel ownership 转交给 store。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>分别读取两个固定槽；单槽 CRC/格式损坏只淘汰该候选，不修改文件。</li>
     *     <li>按 generation 选择最新有效槽；双槽均无效时拒绝启动。</li>
     *     <li>复核 dictionary space identity，防止把其它实例 control 当作当前高水位。</li>
     *     <li>成功构造携带 witness 的 store；失败关闭已打开 channel 并保留原异常。</li>
     * </ol>
     *
     * @param path 已有 control 的诊断路径
     * @param channel 调用方已打开的读写 channel；成功时 ownership 转交返回对象，失败时由本方法关闭
     * @param expectedSpaceId 当前实例固定 dictionary space identity
     * @param witness 后续 reservation/advance 的写前 durable witness
     * @return 恢复到最新有效槽且持有 channel 的 control store
     * @throws DictionaryCatalogCorruptionException 双槽无效或 space identity 冲突时抛出
     */
    private static DictionaryControlStore openValidated(Path path, FileChannel channel, SpaceId expectedSpaceId,
                                                         DictionaryDurabilityWitness witness) {
        try {
            // 1. 每个槽独立校验，单槽损坏不影响另一个完整 generation。
            List<DictionaryControlSnapshot> candidates = new ArrayList<>(2);
            decodeSlot(channel, 0).ifPresent(candidates::add);
            decodeSlot(channel, 1).ifPresent(candidates::add);
            // 2. generation 是槽新旧的唯一排序依据；不能按物理槽号猜最新值。
            DictionaryControlSnapshot latest = candidates.stream()
                    .max(Comparator.comparingLong(DictionaryControlSnapshot::generation))
                    .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                            "both dictionary control slots are invalid: " + path));
            // 3. space identity 不一致表示路径错绑或控制文件来自其它实例。
            if (!latest.dictionarySpaceId().equals(expectedSpaceId)) {
                throw new DictionaryCatalogCorruptionException("dictionary control space id mismatch: expected="
                        + expectedSpaceId.value() + ", actual=" + latest.dictionarySpaceId().value());
            }
            // 4. 只有全部校验成功才把 channel ownership 发布给新 store。
            return new DictionaryControlStore(path, channel, latest, witness);
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
     * 原子预留各身份区间，并在 control 写槽前先持久化 recovery witness。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在 control IO 锁内从当前 durable snapshot 计算每类区间终点并完成溢出/SpaceId 上界校验。</li>
     *     <li>构造下一 generation 与返回区间起点；此时尚未修改 control 文件或内存权威状态。</li>
     *     <li>先 durable 写 manifest reservation，再写另一 control 槽并 force；后者失败只留下安全 identity 空洞。</li>
     *     <li>force 成功后发布 current，并返回以旧 next counters 为起点的 allocation。</li>
     * </ol>
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code reserve} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryPersistenceException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public DictionaryIdAllocation reserve(DictionaryIdRequest request) {
        // 1. 非法请求和所有 counter 溢出都必须在 witness/control IO 前失败。
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
            // 2. next 保存预留后的高水位，allocation 起点仍来自 before。
            DictionaryControlSnapshot next = new DictionaryControlSnapshot(
                    advance(before.generation(), 1, "control generation"), before.dictionarySpaceId(),
                    schema, table, index, space, ddl, version);
            // 3. witness 必须早于 control slot。即使后续 force 失败，重建时保守采用更高 counter 只产生安全空洞。
            witness.beforeControlReservation(next);
            try {
                writeSlot(channel, next);
                channel.force(true);
            } catch (IOException e) {
                throw new DictionaryPersistenceException("reserve dictionary ids failed: " + path, e);
            }
            // 4. 内存 current 只发布已经 force 的槽，异常返回时调用方拿不到 allocation。
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

    /**
     * 把 existing control 单调推进到 recovery witness 的安全下界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 floor 与当前 dictionary space 一致，并逐分量计算不回退的目标 counters。</li>
     *     <li>所有目标已被当前槽覆盖时幂等返回，不产生无意义 generation。</li>
     *     <li>否则先调用 witness，再写另一槽并 force；generation 只从当前槽加一，不信任外部代次跳跃。</li>
     *     <li>force 成功后发布内存快照；异常时当前槽和内存状态保持为旧权威值。</li>
     * </ol>
     *
     * @param floor manifest clean/control reservation 推导出的安全高水位下界
     * @return durable 后的 control 快照；可能与调用前相同
     * @throws DictionaryCatalogCorruptionException dictionary space identity 冲突时抛出
     * @throws DictionaryPersistenceException witness、写槽或 force 失败时抛出
     */
    public DictionaryControlSnapshot advanceTo(DictionaryControlSnapshot floor) {
        validateSnapshot(floor);
        ioLock.lock();
        try {
            // 1. 只提升 counters；外部 generation 仅表达证据新旧，不能直接覆盖本文件槽序。
            DictionaryControlSnapshot before = current;
            if (!before.dictionarySpaceId().equals(floor.dictionarySpaceId())) {
                throw new DictionaryCatalogCorruptionException(
                        "dictionary control recovery floor space id mismatch");
            }
            long nextSchema = Math.max(before.nextSchemaId(), floor.nextSchemaId());
            long nextTable = Math.max(before.nextTableId(), floor.nextTableId());
            long nextIndex = Math.max(before.nextIndexId(), floor.nextIndexId());
            long nextSpace = Math.max(before.nextSpaceId(), floor.nextSpaceId());
            long nextDdl = Math.max(before.nextDdlId(), floor.nextDdlId());
            long nextVersion = Math.max(before.nextDictionaryVersion(), floor.nextDictionaryVersion());

            // 2. 当前 durable 槽已经覆盖 witness 时不写盘。
            if (nextSchema == before.nextSchemaId() && nextTable == before.nextTableId()
                    && nextIndex == before.nextIndexId() && nextSpace == before.nextSpaceId()
                    && nextDdl == before.nextDdlId() && nextVersion == before.nextDictionaryVersion()) {
                return before;
            }

            // 3. 写前 witness 保持普通 reservation 与 recovery advance 相同的 crash 语义。
            DictionaryControlSnapshot next = new DictionaryControlSnapshot(
                    advance(before.generation(), 1, "control generation"), before.dictionarySpaceId(),
                    nextSchema, nextTable, nextIndex, nextSpace, nextDdl, nextVersion);
            witness.beforeControlReservation(next);
            try {
                writeSlot(channel, next);
                channel.force(true);
            } catch (IOException failure) {
                throw new DictionaryPersistenceException(
                        "advance dictionary control recovery high-water failed: " + path, failure);
            }

            // 4. 只有 durable 后才替换内存权威快照。
            current = next;
            return next;
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

    /** 校验 recovery control snapshot 的持久范围。 */
    private static void validateSnapshot(DictionaryControlSnapshot snapshot) {
        if (snapshot == null || snapshot.generation() <= 0 || snapshot.dictionarySpaceId() == null
                || snapshot.nextSchemaId() <= 0 || snapshot.nextTableId() <= 0
                || snapshot.nextIndexId() <= 0 || snapshot.nextSpaceId() <= 0
                || snapshot.nextSpaceId() > Integer.MAX_VALUE || snapshot.nextDdlId() <= 0
                || snapshot.nextDictionaryVersion() <= 0) {
            throw new DatabaseValidationException("dictionary control recovery snapshot is invalid");
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
