package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;
import cn.zhangyis.db.storage.fil.exception.DataFileCorruptedException;
import cn.zhangyis.db.storage.fil.exception.PageOutOfBoundsException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于 {@link java.nio.channels.FileChannel} positional IO 的单文件 {@link PageStore} 实现。
 *
 * <p>职责边界：本类只维护 {@link SpaceId} 到 {@link DataFileHandle} 的进程内路由，并把整页读写、文件扩展、
 * fsync、截断和关闭委托给目标句柄。它不解析 page envelope/record/FSP，不计算 checksum，不判断 tablespace
 * lifecycle，也不执行 WAL、doublewrite、Buffer Pool stale 或 Data Dictionary 准入；这些语义必须由上层在调用前保证。</p>
 *
 * <p>并发与所有权：{@link ConcurrentHashMap} 只保护句柄注册表的原子发布/移除，不保护 FileChannel 本身。
 * 单个文件的普通 IO、size 变化、force 与 close 由 {@link DataFileHandle} 内的
 * TablespaceLifecycleLatch、FileSizeLock 和 FsyncLock 按既定顺序保护。create/open 成功登记后句柄归本 store 所有，
 * 只能通过 {@link #close(SpaceId)} 或 {@link #close()} 释放。</p>
 *
 * <p>可见性：create 必须在文件范围初始化完成后才把句柄放入 map；autoextend/ensureCapacity 必须在新范围初始化完成后
 * 才发布新的页数。read/write 使用 positional IO，不共享 FileChannel position；同一页并发写仍由 Buffer Pool page latch
 * 或 flush snapshot 保证，本层不增加全局写锁。</p>
 *
 * <p>关闭边界：单空间 close 先从 map 移除再 drain/关闭旧句柄，使新查找立即失败；全量 force/close 只处理调用开始时的
 * 快照。恢复和引擎 shutdown 必须先关闭新 create/open 的入口，才能把快照完成视为全局持久化或资源释放屏障。</p>
 *
 * <p>与 MySQL/InnoDB 的差异：当前每个 SpaceId 只对应一个普通文件，不实现多文件 system/general tablespace 路由、
 * mmap、异步 IO 或平台原生预分配；物理范围初始化通过 {@link DataFileGateway} 适配，默认仍是跨平台零填充。</p>
 */
@Slf4j
public final class FileChannelPageStore implements PageStore {

    /**
     * 单次 {@link #extend(SpaceId)} 的增长量策略；只决定新增页数，不执行 IO、不持锁，也不发布文件大小。
     * 默认构造器使用 MySQL 8.0 file-per-table 风格边界；本 store 不读取表空间类型，所以该实例管理的
     * UNDO/GENERAL 文件共享同一策略。
     */
    private final AutoExtendPolicy autoExtendPolicy;

    /**
     * data-file 物理范围初始化网关。调用时 DataFileHandle 已持有正确生命周期/size 锁；gateway 只初始化
     * FileChannel 页范围，不能回调 Buffer Pool、registry、redo 或本 store。默认实现为零填充。
     */
    private final DataFileGateway dataFileGateway;

    /**
     * 当前进程已登记并由本 store 拥有的物理句柄。key 是唯一 SpaceId；map 负责原子发布/移除，句柄内部锁负责 IO。
     * 该映射不是磁盘或数据字典的权威 tablespace catalog，进程重启后必须由上层 discovery 重新 open。
     */
    private final ConcurrentMap<SpaceId, DataFileHandle> handles = new ConcurrentHashMap<>();

    /**
     * 使用默认 IBD 自动扩展策略与零填充网关创建空 PageStore；构造本身不创建线程、文件或句柄。
     */
    public FileChannelPageStore() {
        this(new DefaultIbdAutoExtendPolicy(), new ZeroFillDataFileGateway());
    }

    /**
     * 使用调用方提供的自动扩展策略和默认零填充网关创建空 PageStore。
     *
     * @param autoExtendPolicy 每次 extend 根据当前页数计算正增长量的无状态策略；不能为 {@code null}
     * @throws DatabaseValidationException 策略为空时抛出，不会创建任何文件或句柄
     */
    public FileChannelPageStore(AutoExtendPolicy autoExtendPolicy) {
        this(autoExtendPolicy, new ZeroFillDataFileGateway());
    }

    /**
     * 注入自动扩展策略与物理范围网关的包内构造器，供 FIL 集成测试验证初始化、扩容和失败发布边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在字段发布前校验策略和 gateway，避免构造出只能打开但无法安全扩展的部分对象。</li>
     *     <li>保存不可变协作者；句柄 map 保持为空，后续每个 create/open 独立建立文件所有权。</li>
     * </ol>
     *
     * @param autoExtendPolicy 单次扩展页数策略；不能为 {@code null}
     * @param dataFileGateway 新建/扩容物理范围初始化器；不能为 {@code null}
     * @throws DatabaseValidationException 任一协作者为空时抛出
     */
    FileChannelPageStore(AutoExtendPolicy autoExtendPolicy, DataFileGateway dataFileGateway) {
        // 1. 两个协作者共同决定文件增长能力，必须在发布 store 前一次性校验完整。
        if (autoExtendPolicy == null || dataFileGateway == null) {
            throw new DatabaseValidationException("page store dependencies must not be null");
        }

        // 2. 构造器只保存不可变策略，不触碰文件系统；handles 从空 map 开始由 create/open 原子填充。
        this.autoExtendPolicy = autoExtendPolicy;
        this.dataFileGateway = dataFileGateway;
    }

    /**
     * 创建、初始化并原子登记一个新的单文件表空间句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 SpaceId，并用 {@code containsKey} 快速拒绝当前快照中已登记的 identity；该检查只减少无谓 IO，
     *     并发唯一性最终由 {@code putIfAbsent} 保证。</li>
     *     <li>{@link DataFileHandle#create} 校验路径、页大小和初始页数，以 CREATE_NEW 打开文件，并通过 gateway 初始化
     *     {@code [0, initialSizeInPages)} 全部页；范围初始化完成前句柄不会进入 map。</li>
     *     <li>用 {@code putIfAbsent} 原子发布 SpaceId→handle。当前线程获胜后，本 store 接管 FileChannel 所有权并允许路由 IO。</li>
     *     <li>若输给并发登记，关闭本线程句柄并尽力删除它刚创建的孤儿文件，再抛重复登记异常；删除失败只记录告警，
     *     不掩盖重复 SpaceId。若关闭本身失败则直接保留该物理异常，文件可能残留供上层诊断/清理。</li>
     *     <li>只在句柄成功发布后记录创建日志；日志不代表 fsync，持久化边界仍由上层显式调用 {@link #force(SpaceId)}。</li>
     * </ol>
     *
     * <p>普通 create/open 初始化失败时 DataFileHandle 会关闭未发布 channel，但 CREATE_NEW 文件可能已经存在；
     * 本方法只对“句柄创建成功但并发登记失败”的孤儿执行删除补偿，其他失败由 DDL/recovery 根据受控路径清理。</p>
     *
     * @param spaceId 当前进程内唯一的表空间路由标识；不能为 {@code null}
     * @param path 待创建的新数据文件路径；不能为 {@code null}，目标文件必须不存在
     * @param pageSize 实例页大小，决定文件长度、页偏移和 buffer 契约；不能为 {@code null}
     * @param initialSizeInPages 初始零填充页数；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空或同一 SpaceId 已被/并发被登记时抛出
     * @throws DataFilePhysicalException 文件已存在、CREATE_NEW/范围初始化/关闭失败时抛出；由 IO 引起时保留原异常为 cause
     */
    @Override
    public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
        // 1. SpaceId 先校验并走无竞争快路径；真正并发唯一性不能依赖 containsKey。
        validateSpaceId(spaceId);
        if (handles.containsKey(spaceId)) {
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }

        // 2. CREATE_NEW + 全范围初始化成功后才得到可发布 handle；失败 channel 会关闭，但已创建文件可能残留。
        DataFileHandle handle = DataFileHandle.create(spaceId, path, pageSize, initialSizeInPages, dataFileGateway);

        // 3. putIfAbsent 是 SpaceId 所有权的线性化点，获胜句柄自此由本 store 负责后续 close。
        if (handles.putIfAbsent(spaceId, handle) != null) {
            // 4. 输方先关闭再尽力删除自己的孤儿文件；删除失败只告警，关闭失败则保留更直接的物理资源异常。
            handle.close();
            try {
                Files.deleteIfExists(path);
            } catch (IOException deleteError) {
                log.warn("failed to delete orphan data file after lost create race: {}", path, deleteError);
            }
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }

        // 5. 成功日志只说明运行时句柄已发布，不承诺文件内容已 force 到稳定介质。
        log.info("created tablespace data file: space={} path={}", spaceId.value(), path);
    }

    /**
     * 打开、校验并原子登记一个已存在的单文件表空间句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId，并快速拒绝当前快照中已登记的 identity；并发发布仍以 {@code putIfAbsent} 为准。</li>
     *     <li>{@link DataFileHandle#open} 校验路径存在，以 READ/WRITE 打开 FileChannel，读取文件长度并要求整页对齐；
     *     由长度推导出的物理页数作为句柄初始可见边界。</li>
     *     <li>原子发布句柄；获胜后本 store 接管所有权。输给并发 open 的句柄只关闭，不删除原文件，因为文件并非本次创建。</li>
     *     <li>只在发布成功后记录 open 日志；page0 identity、checksum 与 lifecycle 由上层 metadata loader 校验。</li>
     * </ol>
     *
     * @param spaceId 当前进程内唯一的表空间路由标识；不能为 {@code null}
     * @param path 已存在的数据文件路径；不能为 {@code null}
     * @param pageSize 实例页大小，文件长度必须是其整数倍；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空或同一 SpaceId 已被/并发被登记时抛出
     * @throws DataFilePhysicalException 文件不存在、无法打开或关闭竞争失败句柄时抛出
     * @throws DataFileCorruptedException 文件长度不是 pageSize 整数倍时抛出，普通页定位不能继续
     */
    @Override
    public void open(SpaceId spaceId, Path path, PageSize pageSize) {
        // 1. containsKey 只优化常见重复打开，putIfAbsent 才是并发登记的原子裁决点。
        validateSpaceId(spaceId);
        if (handles.containsKey(spaceId)) {
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }

        // 2. open 只校验物理文件存在/页对齐并推导 size，不解析 page0 或判断逻辑 lifecycle。
        DataFileHandle handle = DataFileHandle.open(spaceId, path, pageSize, dataFileGateway);

        // 3. 获胜句柄进入路由表；输方关闭自己的 FileChannel，但绝不能删除调用前已经存在的数据文件。
        if (handles.putIfAbsent(spaceId, handle) != null) {
            handle.close();
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }

        // 4. 运行时 open 已完成；上层仍须读取 page0 并发布 registry metadata 后才能进入普通空间管理。
        log.info("opened tablespace data file: space={} path={}", spaceId.value(), path);
    }

    /**
     * 把一个完整物理页读取到调用方缓冲区，不改变任何 registry、Buffer Pool 或 page checksum 状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 PageId；buffer 非空/整页契约由目标 handle 按其 PageSize 校验。</li>
     *     <li>从路由表取得当前 handle，再由它在 Lifecycle(S) 下校验页边界并执行 positional read。
     *     成功后 dst position 前进一个 pageSize，limit 不变；调用方负责 flip/rewind。</li>
     * </ol>
     *
     * @param pageId 由 SpaceId 与表空间内 PageNo 组成的物理定位键；不能为 {@code null}
     * @param dst 可写目标缓冲区；不能为 {@code null}，调用时 remaining 必须严格等于该文件 pageSize
     * @throws DatabaseValidationException PageId/dst 为空或缓冲区剩余长度不是整页时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记、已移除或底层句柄已关闭时抛出
     * @throws PageOutOfBoundsException PageNo 不小于句柄当前已发布页数时抛出，调用方可在合法扩容后重试
     * @throws DataFilePhysicalException positional read 失败或遇到意外 EOF 时抛出，底层 IOException 保留为 cause
     */
    @Override
    public void readPage(PageId pageId, ByteBuffer dst) {
        // 1. 物理路由 identity 在访问并发 map 前必须存在。
        validatePageId(pageId);

        // 2. handle 负责 pageSize buffer 契约、lifecycle/size 检查与完整 positional read。
        require(pageId.spaceId()).readPage(pageId.pageNo(), dst);
    }

    /**
     * 将调用方提供的完整页镜像写入指定物理页，不负责 WAL、doublewrite、checksum 或 dirty 状态管理。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 PageId；src 非空/整页契约由目标 handle 按其 PageSize 校验。</li>
     *     <li>路由当前 handle，由它在 Lifecycle(S) 下检查发布 size 并完成 positional write。成功后
     *     src position 前进一个 pageSize。调用方必须已持 page latch/稳定 snapshot，并满足 WAL gate。</li>
     * </ol>
     *
     * @param pageId 目标物理页定位键；不能为 {@code null}
     * @param src 完整页镜像；不能为 {@code null}，调用时 remaining 必须严格等于该文件 pageSize
     * @throws DatabaseValidationException PageId/src 为空或缓冲区剩余长度不是整页时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记、已移除或底层句柄已关闭时抛出
     * @throws PageOutOfBoundsException PageNo 超出当前已发布物理边界时抛出，禁止借 write 隐式扩展文件
     * @throws DataFilePhysicalException positional write 失败时抛出，调用方必须保留 dirty 状态并进入重试/故障路径
     */
    @Override
    public void writePage(PageId pageId, ByteBuffer src) {
        // 1. 无 PageId 时不能选择 data file 或页偏移。
        validatePageId(pageId);

        // 2. handle 完成整页 buffer、生命周期、边界与 positional write；上层继续拥有 WAL/dirty 语义。
        require(pageId.spaceId()).writePage(pageId.pageNo(), src);
    }

    /**
     * 按注入策略对单文件表空间执行一次自动扩展，并返回扩展后的已发布页数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId 并要求路由表已登记 handle。</li>
     *     <li>把策略交给 handle；handle 按 Lifecycle(S)→FileSize(X) 读取旧大小、计算正增量、初始化尾部，
     *     最后 volatile 发布新页数，返回值再包装为 PageNo。</li>
     * </ol>
     *
     * <p>方法不 force；范围初始化中途失败时文件可能物理增长，但发布 size 保持旧值，新尾部对 read
     * 不可见，后续 retry/recovery 可重复初始化并收敛。</p>
     *
     * @param spaceId 已打开且允许上层执行 autoextend 的表空间标识；不能为 {@code null}
     * @return 本次扩展成功后句柄发布的新物理容量页数
     * @throws DatabaseValidationException SpaceId 为空或策略返回小于 1 的增量时抛出
     * @throws TablespaceNotOpenException 目标句柄未登记或已关闭时抛出
     * @throws DataFilePhysicalException 新物理范围初始化失败时抛出；本方法不会自动缩回可能已增长的文件
     */
    @Override
    public PageNo extend(SpaceId spaceId) {
        // 1. 先验证路由键，再取得当前已登记 handle。
        validateSpaceId(spaceId);

        // 2. handle 串行执行一次策略增长；成功页数转换为领域值返回。
        return PageNo.of(require(spaceId).autoExtend(autoExtendPolicy));
    }

    /**
     * 返回目标句柄当前已发布的页数快照，供物理越界检查和上层容量诊断使用。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验并路由 SpaceId。</li>
     *     <li>读取 handle volatile size 并包装为 PageNo。返回后可能被 extend/ensureCapacity 增大或
     *     被独占 truncate 缩小；它不是锁定快照，也不读取 page0 FSP 容量。</li>
     * </ol>
     *
     * @param spaceId 已登记表空间标识；不能为 {@code null}
     * @return 当前句柄允许 read/write 的物理页数边界快照
     * @throws DatabaseValidationException SpaceId 为空时抛出
     * @throws TablespaceNotOpenException 目标句柄未登记时抛出
     */
    @Override
    public PageNo currentSizeInPages(SpaceId spaceId) {
        // 1. 查询只接受有效且已登记的空间身份。
        validateSpaceId(spaceId);

        // 2. 单次 volatile 读取转换为不可变 PageNo，不锁定后续尺寸变化。
        return PageNo.of(require(spaceId).currentSizeInPages());
    }

    /**
     * 返回 create/open 时绑定到句柄的数据文件路径，不访问文件系统也不触发 metadata loader。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId 并要求 handle 已登记。</li>
     *     <li>返回 handle 的不可变路径字段；不重新打开或检查文件。</li>
     * </ol>
     *
     * @param spaceId 已登记表空间标识；不能为 {@code null}
     * @return 句柄生命周期内保持不变的路径对象；仅用于 metadata/discovery/诊断，实际 IO 仍走已打开 FileChannel
     * @throws DatabaseValidationException SpaceId 为空时抛出
     * @throws TablespaceNotOpenException 目标句柄未登记时抛出
     */
    @Override
    public Path pathOf(SpaceId spaceId) {
        // 1. 无效或未登记 identity 不能产生可信 metadata 路径。
        validateSpaceId(spaceId);

        // 2. 直接返回 handle 绑定路径，不执行文件系统 IO。
        return require(spaceId).path();
    }

    /**
     * 对一个已打开数据文件执行 {@code FileChannel.force(true)}，建立数据与文件 metadata 的明确持久化边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验并路由 SpaceId。</li>
     *     <li>handle 持 Lifecycle(S) 与 per-file FsyncLock 执行 force；close/truncate 的 X 闩会等待。
     *     本方法不检查 WAL 或推进 checkpoint，调用方必须先保证 redo durable 和镜像稳定。</li>
     * </ol>
     *
     * @param spaceId 待强制持久化的已登记表空间；不能为 {@code null}
     * @throws DatabaseValidationException SpaceId 为空时抛出
     * @throws TablespaceNotOpenException 目标句柄未登记或已关闭时抛出
     * @throws DataFilePhysicalException force/fsync 失败时抛出，调用方不能把该文件视为 durable
     */
    @Override
    public void force(SpaceId spaceId) {
        // 1. 只允许对有效且已登记的物理文件建立 durable 边界。
        validateSpaceId(spaceId);

        // 2. handle 负责生命周期与同文件 fsync 串行化。
        require(spaceId).force();
    }

    /**
     * 对调用开始时已登记的全部句柄执行 force，并在尝试所有文件后汇总失败。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>复制 {@code handles.values()} 建立有限快照，避免遍历期间 map 结构变化导致漏遍历或重复处理同一句柄。</li>
     *     <li>逐句柄调用 per-file force；单个失败只记录异常，继续尝试其余文件，避免首个坏文件阻止其他恢复结果落盘。</li>
     *     <li>全部尝试结束后，若存在失败则以第一个异常为 cause 构造 {@link DataFilePhysicalException}，其余失败作为
     *     suppressed exception 保留；无失败才代表该快照内所有文件完成 force。</li>
     * </ol>
     *
     * <p>并发 create/open 在快照之后发布的句柄不属于本次屏障。crash recovery 与 shutdown 必须先关闭新注册入口，
     * 再调用本方法，才能把成功返回解释为全局 data-file durable。</p>
     *
     * @throws DataFilePhysicalException 一个或多个快照句柄 force 失败时汇总抛出；所有句柄都已经被尝试
     */
    @Override
    public void forceAll() {
        // 1. 先复制句柄快照，固定本次持久化屏障的参与者集合；之后的新注册不隐式加入本轮。
        List<RuntimeException> errors = new ArrayList<>();
        for (DataFileHandle handle : new ArrayList<>(handles.values())) {
            // 2. 每个文件独立 force；失败仅累积，确保其余恢复写仍有机会持久化。
            try {
                handle.force();
            } catch (RuntimeException e) {
                errors.add(e);
            }
        }

        // 3. 全部参与者尝试完成后统一报告，首因 + suppressed 保留每个失败文件的诊断链。
        if (!errors.isEmpty()) {
            DataFilePhysicalException aggregate = new DataFilePhysicalException(
                    "failed to force " + errors.size() + " tablespace handle(s)", errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    /**
     * 将经过上层 lifecycle/WAL/Buffer Pool 排空校验的单文件缩短请求路由到目标句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验并路由 SpaceId；目标页数交给 handle 校验。</li>
     *     <li>handle 按 Lifecycle(X)→FileSize(X)→FsyncLock drain 普通 IO 后 truncate，立即收紧发布页数并
     *     force。force 失败时保留目标边界，上层 marker/recovery 必须续作。</li>
     * </ol>
     *
     * @param spaceId 已完成上层独占准入和 dirty/fixed frame 排空的表空间；不能为 {@code null}
     * @param targetSizeInPages 严格小于当前大小的正目标页数；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空、目标非正或目标不小于当前大小时抛出
     * @throws TablespaceNotOpenException 目标句柄未登记或已关闭时抛出
     * @throws DataFilePhysicalException truncate 或随后的 force 失败时抛出；调用方不得恢复旧逻辑边界
     */
    @Override
    public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
        // 1. 表空间 identity 在进入可能 drain 的 handle 路径前校验。
        validateSpaceId(spaceId);

        // 2. handle 负责目标范围、锁序、物理 truncate、size 发布与 force。
        require(spaceId).truncateTo(targetSizeInPages);
    }

    /**
     * 幂等地把单文件物理容量扩到至少指定页数，只增不减，主要供 recovery 对齐 page0 恢复出的逻辑大小。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验并路由 SpaceId；最小页数交由 handle 做正数校验。</li>
     *     <li>handle 在目标不大于当前 size 时 no-op，否则持 Lifecycle(S)+FileSize(X) 初始化
     *     {@code [current,target)} 后发布新 size。本方法不 force。</li>
     * </ol>
     *
     * @param spaceId 待扩容的已登记表空间；不能为 {@code null}
     * @param minSizeInPages 期望的最小物理页数，必须为正；小于等于当前大小表示 no-op
     * @throws DatabaseValidationException 参数为空或目标非正时抛出
     * @throws TablespaceNotOpenException 目标句柄未登记或已关闭时抛出
     * @throws DataFilePhysicalException 新增物理范围初始化失败时抛出；新 size 不会在失败前发布
     */
    @Override
    public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
        // 1. 先确保容量协调请求绑定有效且已登记表空间。
        validateSpaceId(spaceId);

        // 2. handle 串行检查/建立尾部，失败时不会推进发布 size。
        require(spaceId).ensureCapacity(minSizeInPages);
    }

    /**
     * 从路由表移除并关闭一个表空间句柄；目标未登记时幂等 no-op。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId，并用 map.remove 先撤销新查找入口；从此新的 read/write/force 会按未打开失败。</li>
     *     <li>若旧句柄存在，DataFileHandle 获取 Lifecycle(X) 等待已取得 Lifecycle(S) 的 IO/force 离开，置 closed 后关闭
     *     FileChannel。并发线程若已在 remove 前拿到句柄，也会由 lifecycle latch 保证完成或看到 closed；关闭失败时句柄
     *     仍保持从 map 移除且 closed=true 的 fail-closed 状态，异常向上传播，本 store 不重新登记坏句柄。</li>
     * </ol>
     *
     * <p>本方法不删除文件、不修改 lifecycle marker，也不主动清理 Buffer Pool；调用方必须在 DROP/truncate 编排中先完成这些上层协议。</p>
     *
     * @param spaceId 待撤销运行时路由的表空间标识；不能为 {@code null}
     * @throws DatabaseValidationException SpaceId 为空时抛出
     * @throws DataFilePhysicalException 已登记句柄关闭 FileChannel 失败时抛出；句柄不会重新进入 map
     */
    @Override
    public void close(SpaceId spaceId) {
        // 1. remove 是撤销路由所有权的线性化点，先阻止新的 require 再处理可能较慢的生命周期 drain/close。
        validateSpaceId(spaceId);
        DataFileHandle handle = handles.remove(spaceId);

        // 2. 目标未登记时保持幂等；存在时由句柄 Lifecycle(X) drain 旧 IO 并关闭 FileChannel。
        if (handle != null) {
            handle.close();
        }
    }

    /**
     * 关闭调用开始时已登记的全部句柄；单个关闭失败不会阻止其余 FileChannel 释放，最终统一报告。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>复制当前 key 集合固定本轮关闭参与者；shutdown 编排必须已阻止新的 create/open，否则快照之后的句柄不会被关闭。</li>
     *     <li>逐 SpaceId 调用 {@link #close(SpaceId)}，利用 remove→Lifecycle(X) close 保证同一句柄最多由一个线程取得关闭所有权；
     *     单个失败只记录，继续关闭其余句柄。</li>
     *     <li>全部尝试后，以第一个关闭失败为 cause、其余为 suppressed 构造聚合异常；无失败才表示快照句柄全部移除并关闭。</li>
     * </ol>
     *
     * @throws DataFilePhysicalException 一个或多个快照句柄关闭失败时汇总抛出；其余句柄仍已被尝试关闭
     */
    @Override
    public void close() {
        // 1. 快照 key 固定本轮参与者集合；调用方必须在外部 gate 保证此后不再注册新句柄。
        List<RuntimeException> errors = new ArrayList<>();
        for (SpaceId spaceId : new ArrayList<>(handles.keySet())) {
            // 2. 单空间 close 先 remove 再 drain；失败累积但不阻止其它 FileChannel 释放。
            try {
                close(spaceId);
            } catch (RuntimeException e) {
                errors.add(e);
            }
        }

        // 3. 关闭尝试全部完成后聚合诊断，保留首因与其它 suppressed failure。
        if (!errors.isEmpty()) {
            DataFilePhysicalException aggregate = new DataFilePhysicalException(
                    "failed to close " + errors.size() + " tablespace handle(s)", errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    /**
     * 从当前路由快照取得句柄，不触发磁盘 discovery/open，也不检查 registry lifecycle。
     *
     * @param spaceId 已由公开入口完成非空校验的表空间标识
     * @return 当前 map 中由本 store 拥有的句柄引用；引用返回后仍由句柄 lifecycle latch 处理并发 close
     * @throws TablespaceNotOpenException 未登记、已先行 remove 或从未 open/create 时抛出
     */
    private DataFileHandle require(SpaceId spaceId) {
        DataFileHandle handle = handles.get(spaceId);
        if (handle == null) {
            throw new TablespaceNotOpenException("tablespace not open: " + spaceId.value());
        }
        return handle;
    }

    /**
     * 校验物理页路由键存在；SpaceId/PageNo 的数值边界由不可变值对象自身保证。
     *
     * @param pageId 待读写的物理页 identity
     * @throws DatabaseValidationException PageId 为空时抛出
     */
    private void validatePageId(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
    }

    /**
     * 校验表空间路由键存在；正值等领域边界由 {@link SpaceId} 构造时保证。
     *
     * @param spaceId 待注册、查询或关闭的表空间 identity
     * @throws DatabaseValidationException SpaceId 为空时抛出
     */
    private void validateSpaceId(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
