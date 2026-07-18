package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.DataFileCorruptedException;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;
import cn.zhangyis.db.storage.fil.exception.PageOutOfBoundsException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;
import cn.zhangyis.db.storage.fil.lock.FileSizeLock;
import cn.zhangyis.db.storage.fil.lock.FsyncLock;
import cn.zhangyis.db.storage.fil.lock.ResourceGuard;
import cn.zhangyis.db.storage.fil.lock.TablespaceLifecycleLatch;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 一个 {@link SpaceId} 的单个已打开物理数据文件 handle。
 *
 * <p>本类拥有 {@link FileChannel} 从 create/open 成功到 close 的生命周期，按
 * {@code pageNo * pageSize} 执行 positional 整页 IO。它不解析页 envelope/body、不验证 checksum，
 * 不理解 FSP/segment/record，也不生成 redo。</p>
 *
 * <p>并发与锁序：read/write/autoExtend/ensureCapacity/force 持 lifecycle shared；truncate/close 持
 * lifecycle exclusive。尺寸变更随后持 file-size lock，force 调用再持 fsync lock。生命周期闩防止
 * channel 使用与关闭/截断交叉；file-size lock 串行尺寸发布；fsync lock 只串行同文件 force。
 * 同页内容冲突仍由上层 page latch 或稳定 flush snapshot 负责。</p>
 *
 * <p>{@link #currentSizeInPages} 是“已完整初始化且允许本 handle 访问”的发布边界，不总等于
 * {@link FileChannel#size()}：扩展失败可留下未发布物理尾部；truncate 已成功但 force 失败时则立即收紧发布边界。
 * 新尾部在 gateway 完成后才通过 volatile 写发布，读线程看到新 size 时也能看到此前初始化写入。</p>
 *
 * <p>当前简化为 SpaceId 对应单文件，不支持句柄在线替换、page-range lock、异步 IO 或多文件路由。</p>
 */
final class DataFileHandle implements AutoCloseable {

    /**
     * handle 绑定的稳定表空间身份，用于异常诊断和构造 {@link PageId} 做偏移换算；构造后不可变。
     */
    private final SpaceId spaceId;

    /**
     * create/open 时绑定的数据文件路径。后续 IO 只使用已打开 channel；该值供诊断和 page0 loader
     * 重建 metadata 文件描述，不作为重新打开授权。
     */
    private final Path path;

    /**
     * 文件整个 handle 生命周期使用的固定页大小，决定整页 buffer 契约、页偏移和长度对齐。
     */
    private final PageSize pageSize;

    /**
     * 本 handle 独占 ownership 的读写 channel。普通使用受 {@link #lifecycleLatch} shared 保护，
     * close 在 exclusive 下关闭且不会替换该引用。
     */
    private final FileChannel channel;

    /**
     * 新建/新增页范围的物理建立协作者。handle 负责锁和 size 发布，gateway 只能在给定 channel
     * 范围内初始化；失败可能留下未发布尾部，但不能推进 {@link #currentSizeInPages}。
     */
    private final DataFileGateway gateway;

    /**
     * channel 操作与 truncate/close 的首层物理闩；所有其它 handle 内部锁都在取得它之后获取。
     */
    private final TablespaceLifecycleLatch lifecycleLatch = new TablespaceLifecycleLatch();

    /**
     * extend/ensureCapacity/truncate 的尺寸变更排他锁，保护旧边界读取、尾部建立和新边界发布这一序列。
     */
    private final FileSizeLock fileSizeLock = new FileSizeLock();

    /**
     * 单文件 fsync permit。只串行 {@link FileChannel#force(boolean)}，不执行 WAL gate、checkpoint
     * 或上层回调；truncate 按 lifecycle→file-size→fsync 获取。
     */
    private final FsyncLock fsyncLock = new FsyncLock();

    /**
     * 已完整建立并发布给页 IO 的 exclusive 页边界。写入只在 file-size lock 下发生，读路径以 volatile
     * 快照做越界检查；它可能小于 channel 实际尾部，但绝不能大于可完整读取的初始化范围。
     */
    private volatile long currentSizeInPages;

    /**
     * handle 关闭的单向状态。close 在 lifecycle exclusive 下从 false 置 true，且即使 channel.close 失败
     * 也保持 true；普通路径在 shared 下检查，禁止失败关闭后的句柄重新使用。
     */
    private volatile boolean closed;

    /**
     * 接管一个已打开且几何已验证的 channel。
     *
     * <p>构造器不重复校验参数；只能由本类 create/open 在范围初始化或文件长度校验成功后调用。</p>
     *
     * @param spaceId channel 内页面所属的稳定空间标识
     * @param path channel 对应诊断路径
     * @param pageSize 文件固定页大小
     * @param channel 由新 handle 接管的已打开读写 channel
     * @param currentSizeInPages 已完整初始化并可发布的非负页数
     * @param gateway 后续尺寸增长复用的非空范围 gateway
     */
    private DataFileHandle(SpaceId spaceId, Path path, PageSize pageSize, FileChannel channel,
                           long currentSizeInPages, DataFileGateway gateway) {
        this.spaceId = spaceId;
        this.path = path;
        this.pageSize = pageSize;
        this.channel = channel;
        this.currentSizeInPages = currentSizeInPages;
        this.gateway = gateway;
    }

    /**
     * 使用默认零填充 gateway 创建新数据文件。
     *
     * @param spaceId 非空表空间身份
     * @param path 非空目标路径，文件必须不存在
     * @param pageSize 非空固定页大小
     * @param initialSizeInPages 非空非负初始页数
     * @return 已接管 channel、但尚未由 PageStore 路由表发布的 handle
     * @throws DatabaseValidationException 参数非法时抛出
     * @throws DataFilePhysicalException 文件已存在、无法创建或初始范围建立失败时抛出
     */
    static DataFileHandle create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
        return create(spaceId, path, pageSize, initialSizeInPages, new ZeroFillDataFileGateway());
    }

    /**
     * 创建新文件，通过指定 gateway 建立初始范围，并在全部成功后构造 handle。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 identity/path/PageSize、初始页数和 gateway；此时不触碰文件。</li>
     *     <li>在打开前快速拒绝已存在路径；最终排他创建仍由 {@code CREATE_NEW} 保证。</li>
     *     <li>以 READ/WRITE 创建 channel，并让 gateway 建立 {@code [0,initialSize)}；handle 尚不可见。</li>
     *     <li>范围完整成功后才把初始 size 与 channel 封装进 handle 返回；方法不执行 force。</li>
     *     <li>IO 或运行时失败均尽力关闭尚未转交的 channel，再保留原异常向上抛出；已创建路径和部分文件内容
     *     不在本方法删除，由外层 DDL 清理。</li>
     * </ol>
     *
     * @param spaceId 非空表空间身份
     * @param path 非空目标路径，文件必须不存在
     * @param pageSize 非空固定页大小
     * @param initialSizeInPages 非空非负初始页数；零会建立空文件 handle
     * @param gateway 非空初始/增长范围 gateway
     * @return 初始范围完整建立、拥有 channel 的未发布 handle
     * @throws DatabaseValidationException 参数非法时抛出
     * @throws DataFilePhysicalException 路径已存在、创建或范围 IO 失败时抛出
     */
    static DataFileHandle create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages,
                                 DataFileGateway gateway) {
        // 1. 在任何文件系统副作用前建立完整构造参数。
        validate(spaceId, path, pageSize);
        if (initialSizeInPages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        if (gateway == null) {
            throw new DatabaseValidationException("data file gateway must not be null");
        }

        // 2. 快速拒绝已有路径；并发竞争仍由 CREATE_NEW 做最终裁决。
        if (Files.exists(path)) {
            throw new DataFilePhysicalException("data file already exists: " + path);
        }
        FileChannel channel = null;
        try {
            // 3. channel 尚未转交；gateway 完整建立初始左闭右开页范围。
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long pages = initialSizeInPages.value();
            gateway.initialize(channel, 0, pages, pageSize, path);

            // 4. 只有初始化成功才发布 handle 内 size；durability 仍需上层显式 force。
            return new DataFileHandle(spaceId, path, pageSize, channel, pages, gateway);
        } catch (IOException e) {
            // 5. 失败路径只回收 channel；物理路径/部分尾部保留给外层受控清理。
            closeQuietly(channel);
            throw new DataFilePhysicalException("create data file failed: " + path, e);
        } catch (RuntimeException e) {
            closeQuietly(channel);
            throw e;
        }
    }

    /**
     * 使用默认零填充 gateway 打开已有数据文件。
     *
     * @param spaceId 非空表空间身份
     * @param path 非空已有文件路径
     * @param pageSize 非空固定页大小
     * @return 按物理长度推导 size 的未发布 handle
     * @throws DatabaseValidationException 参数非法时抛出
     * @throws DataFilePhysicalException 文件不存在或无法打开/读取长度时抛出
     * @throws DataFileCorruptedException 文件长度不按 pageSize 对齐时抛出
     */
    static DataFileHandle open(SpaceId spaceId, Path path, PageSize pageSize) {
        return open(spaceId, path, pageSize, new ZeroFillDataFileGateway());
    }

    /**
     * 打开已有文件、校验页几何，并绑定后续增长 gateway。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 identity/path/PageSize/gateway，并在打开前快速拒绝不存在路径。</li>
     *     <li>以 READ/WRITE 打开 channel，读取当前字节长度。</li>
     *     <li>要求长度可被 pageSize 整除；不读取 page0 或 checksum，零长度也通过几何校验。</li>
     *     <li>用精确页数构造 handle；gateway 不触碰已有字节，只服务后续 extend/ensureCapacity。</li>
     *     <li>任一失败尽力关闭尚未转交 channel；损坏/校验异常原样传播，IO 包装为物理异常。</li>
     * </ol>
     *
     * @param spaceId 非空表空间身份
     * @param path 非空已有文件路径
     * @param pageSize 非空固定页大小，必须匹配文件格式
     * @param gateway 非空后续增长范围 gateway
     * @return 完成长度对齐校验并拥有 channel 的未发布 handle
     * @throws DatabaseValidationException 参数非法时抛出
     * @throws DataFilePhysicalException 文件不存在或 open/size 读取失败时抛出
     * @throws DataFileCorruptedException 文件长度不按 pageSize 对齐时抛出
     */
    static DataFileHandle open(SpaceId spaceId, Path path, PageSize pageSize, DataFileGateway gateway) {
        // 1. 完整参数在打开 channel 之前校验，路径存在检查仅是快速诊断。
        validate(spaceId, path, pageSize);
        if (gateway == null) {
            throw new DatabaseValidationException("data file gateway must not be null");
        }
        if (!Files.exists(path)) {
            throw new DataFilePhysicalException("data file not found: " + path);
        }
        FileChannel channel = null;
        try {
            // 2. 打开后先读取物理字节长度，尚未把 channel 转交给 handle。
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long length = channel.size();
            int pageBytes = pageSize.bytes();

            // 3. 非整页尾部无法建立安全 pageNo 偏移，必须 fail-closed；本层不检查页内容。
            if (length % pageBytes != 0) {
                closeQuietly(channel);
                throw new DataFileCorruptedException("data file not page-aligned: " + path + " length=" + length);
            }

            // 4. 精确页数成为初始发布边界，gateway 仅留给后续增长路径。
            return new DataFileHandle(spaceId, path, pageSize, channel, length / pageBytes, gateway);
        } catch (IOException e) {
            // 5. 尚未返回的 channel 由本方法尽力回收，底层 IO 根因保留。
            closeQuietly(channel);
            throw new DataFilePhysicalException("open data file failed: " + path, e);
        } catch (RuntimeException e) {
            closeQuietly(channel);
            throw e;
        }
    }

    /**
     * 在共享生命周期内把一个已发布物理页读满到目标 buffer。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在拿文件锁前验证 buffer remaining 恰好为 pageSize，非法调用不阻塞 close/truncate。</li>
     *     <li>取得 lifecycle shared，保证 channel 在本次 IO 完成前不会被 close/truncate。</li>
     *     <li>在闩内检查 closed，并以 volatile size 校验 pageNo、换算物理偏移。</li>
     *     <li>用 positional read 推进 dst position 直到一页读完；不 flip、不校验页内容，异常退出时
     *     shared guard 自动释放。</li>
     * </ol>
     *
     * @param pageNo 表空间内非空页号，必须严格小于当前已发布 size
     * @param dst 非空可写目标 buffer，remaining 必须精确等于 pageSize
     * @throws DatabaseValidationException buffer、pageNo 或 remaining 非法时抛出
     * @throws TablespaceNotOpenException handle 已关闭时抛出
     * @throws PageOutOfBoundsException pageNo 超出当前发布边界时抛出
     * @throws DataFilePhysicalException 读取失败或遇到意外 EOF 时抛出
     */
    void readPage(PageNo pageNo, ByteBuffer dst) {
        // 1. buffer 契约先于文件锁校验，避免错误请求占用物理生命周期资源。
        requirePageSized(dst);

        // 2. shared 闩阻止 close/truncate 在本次 positional read 中途改变 channel/文件尾。
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            // 3. 在闩内复核关闭状态和 volatile 发布边界，并生成稳定页偏移。
            ensureOpen();
            long offset = boundedOffset(pageNo);

            // 4. 消耗一个完整页到 dst；guard 在成功或异常路径都会关闭。
            readFully(dst, offset);
        }
    }

    /**
     * 在共享生命周期内把源 buffer 的一个完整页写入已发布物理页。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在拿锁前要求 src remaining 精确为 pageSize。</li>
     *     <li>取得 lifecycle shared，排斥 close/truncate，但允许其它普通页 IO 和文件尾扩展并发。</li>
     *     <li>检查 handle 未关闭并校验 pageNo 位于 volatile 发布边界内。</li>
     *     <li>用 positional write 消耗 src 的完整一页；不 force、不执行 WAL/checksum，同页互斥由上层
     *     page latch 或稳定 flush snapshot 保证。</li>
     * </ol>
     *
     * @param pageNo 表空间内非空页号，必须严格小于当前已发布 size
     * @param src 非空源 buffer，remaining 必须精确等于 pageSize
     * @throws DatabaseValidationException buffer、pageNo 或 remaining 非法时抛出
     * @throws TablespaceNotOpenException handle 已关闭时抛出
     * @throws PageOutOfBoundsException pageNo 超出当前发布边界时抛出
     * @throws DataFilePhysicalException positional write 失败时抛出；可能已写入部分页
     */
    void writePage(PageNo pageNo, ByteBuffer src) {
        // 1. 错误页镜像在进入物理锁域前失败。
        requirePageSized(src);

        // 2. shared 闩保证写入期间 channel 不关闭、文件尾不被 truncate。
        try (ResourceGuard ignored = lifecycleLatch.acquireShared()) {
            // 3. 在闩内验证 handle 状态与页发布边界。
            ensureOpen();
            long offset = boundedOffset(pageNo);

            // 4. 只完成整页物理写入；durability 与 WAL gate 由上层负责。
            writeFully(src, offset);
        }
    }

    /**
     * 按策略执行一次文件尾增长，并在完整初始化后发布新物理页数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先拒绝空策略，避免取得文件锁后才发现无扩展决策。</li>
     *     <li>按 lifecycle shared→file-size 排他顺序获取 guard，并在锁内复核 handle 未关闭；
     *     shared 允许已有页 IO 并发，file-size 串行所有尺寸变化。</li>
     *     <li>读取旧发布边界，调用策略计算正增量；非正结果 fail-closed。</li>
     *     <li>让 gateway 建立 {@code [oldSize,oldSize+increment)}。旧页 IO 只落在旧范围，与新增尾部不重叠。</li>
     *     <li>gateway 完整成功后 volatile 发布新 size 并返回；不 force、不更新 page0/FSP/registry。</li>
     * </ol>
     *
     * <p>可见性不变量：读线程要么看到旧 size 并拒绝新页，要么通过 volatile 读看到新 size；后者与此前
     * gateway 初始化形成 happens-before，不会读到尚未完整发布的尾部。gateway 失败可能已扩大
     * channel，但 size 保持 oldSize，重试会从旧边界重新建立范围。启动恢复通过
     * {@link #ensureCapacity(PageNo)} 按 redo/page0 逻辑边界收敛物理短文件。</p>
     *
     * @param policy 非空纯扩展增量策略
     * @return 本次增长后新发布的正物理页数
     * @throws DatabaseValidationException policy 为空、策略返回非正增量或目标范围溢出/非法时抛出
     * @throws TablespaceNotOpenException handle 已关闭时抛出
     * @throws DataFilePhysicalException gateway 建立新增范围失败时抛出；发布 size 保持旧值
     */
    long autoExtend(AutoExtendPolicy policy) {
        // 1. 决策协作者必须在进入文件锁域前存在。
        if (policy == null) {
            throw new DatabaseValidationException("auto extend policy must not be null");
        }

        // 2. 生命周期 shared 排斥 truncate/close，file-size 排他串行 extend/ensure/truncate。
        try (ResourceGuard s = lifecycleLatch.acquireShared(); ResourceGuard x = fileSizeLock.acquire()) {
            ensureOpen();

            // 3. 策略基于锁内旧边界计算，返回值必须严格为正。
            long oldSize = currentSizeInPages;
            long inc = policy.nextIncrementPages(oldSize, pageSize);
            if (inc < 1) {
                throw new DatabaseValidationException("auto extend increment must be >= 1: " + inc);
            }

            // 4. 新范围在发布前不可由 read/write 的 boundedOffset 访问。
            gateway.ensureAllocated(channel, oldSize, oldSize + inc, pageSize, path);

            // 5. volatile 写最后发布完整范围；本方法不承担 force 或逻辑容量元数据更新。
            currentSizeInPages = oldSize + inc;
            return currentSizeInPages;
        }
    }

    /**
     * 读取当前已发布的物理页访问边界。
     *
     * <p>返回 volatile 瞬时值，调用后可被并发 autoExtend/ensureCapacity 增大，或在外层独占生命周期
     * 协议下被 truncate 缩小。该值可能小于 channel 实际长度，因为未发布尾部不属于可访问范围。</p>
     *
     * @return 调用时观察到的非负 exclusive 页边界
     */
    long currentSizeInPages() {
        return currentSizeInPages;
    }

    /**
     * 返回 create/open 时绑定且在 handle 生命周期内不变的路径值。
     *
     * @return channel 对应的非空路径；返回值不转移文件 ownership
     */
    Path path() {
        return path;
    }

    /**
     * 对当前数据文件执行一次串行化的 {@code force(true)}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 lifecycle shared→fsync permit 获取，允许页 IO 并发，但排斥 truncate/close，并把同文件
     *     多个 force 串行化。</li>
     *     <li>在锁内复核 handle 未关闭；失败时不调用 channel。</li>
     *     <li>执行 {@code force(true)}，成功建立当前 channel 修改的物理 durable 边界；IOException 包装后
     *     guard 仍按逆序释放。</li>
     * </ol>
     *
     * <p>方法不检查 redo durable LSN、不推进 checkpoint，也不回调 Buffer Pool/redo；调用方负责进入前的
     * WAL 或恢复阶段条件。</p>
     *
     * @throws TablespaceNotOpenException handle 已关闭时抛出
     * @throws DataFilePhysicalException channel force 失败时抛出并保留 IO 根因
     */
    void force() {
        // 1. shared 生命周期保护 channel，fsync permit 只限制同文件 force 并发数。
        try (ResourceGuard lifecycle = lifecycleLatch.acquireShared(); ResourceGuard fsync = fsyncLock.acquire()) {
            // 2. close 只能在 lifecycle exclusive 下置位，因此本次 shared 内检查后状态稳定。
            ensureOpen();
            try {
                // 3. metadata=true 同时请求内容与文件 metadata durable；WAL 前置由上层保证。
                channel.force(true);
            } catch (IOException e) {
                throw new DataFilePhysicalException("force data file failed: " + path, e);
            }
        }
    }

    /**
     * 把单文件物理长度缩短到严格更小的正数页边界并 force。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在取得锁前拒绝空或非正目标。</li>
     *     <li>按 lifecycle exclusive→file-size→fsync 获取，drain 全部普通 IO/extend/force，并在锁内确认
     *     handle 仍打开。</li>
     *     <li>要求目标严格小于当前发布边界，并用精确乘法换算目标字节长度；失败不改变文件。</li>
     *     <li>执行 channel truncate；成功后物理尾部已经不可逆消失，必须立即把 volatile size 收紧到目标，
     *     防止释放锁后按旧边界读到 EOF。</li>
     *     <li>在仍持全部锁时 {@code force(true)}。若该阶段失败，size 仍保持目标，上层 durable
     *     {@code TRUNCATING} marker/recovery 负责续作；任一异常都按逆序释放 guard。</li>
     * </ol>
     *
     * @param targetSizeInPages 非空、严格小于当前发布大小的正页数
     * @throws DatabaseValidationException 目标为空、非正、不小于当前大小或字节换算溢出时抛出
     * @throws TablespaceNotOpenException handle 已关闭时抛出
     * @throws DataFilePhysicalException truncate 或随后 force 失败时抛出
     */
    void truncateTo(PageNo targetSizeInPages) {
        // 1. 非正目标不能表示保留 page0/最小文件几何，在进入 drain 前拒绝。
        if (targetSizeInPages == null || targetSizeInPages.value() < 1) {
            throw new DatabaseValidationException("truncate target size must be positive");
        }

        // 2. exclusive 生命周期先 drain 全部 shared owner，再串行 size 与 fsync 变化。
        try (ResourceGuard lifecycle = lifecycleLatch.acquireExclusive();
             ResourceGuard size = fileSizeLock.acquire();
             ResourceGuard fsync = fsyncLock.acquire()) {
            ensureOpen();

            // 3. truncate 只允许真正缩短，并在触碰 channel 前完成溢出安全字节换算。
            long target = targetSizeInPages.value();
            long current = currentSizeInPages;
            if (target >= current) {
                throw new DatabaseValidationException("truncate target must be smaller than current size: target="
                        + target + ", current=" + current);
            }
            final long targetBytes;
            try {
                targetBytes = Math.multiplyExact(target, (long) pageSize.bytes());
            } catch (ArithmeticException overflow) {
                throw new DatabaseValidationException("truncate target byte offset overflow: " + target, overflow);
            }
            try {
                // 4. 物理缩短成功后立即收紧可访问边界；不能等待 force 成功才发布。
                channel.truncate(targetBytes);
                currentSizeInPages = target;

                // 5. 在全部物理锁内请求 durable；失败时保留收紧 size 供恢复安全续作。
                channel.force(true);
            } catch (IOException e) {
                throw new DataFilePhysicalException("truncate data file failed: " + path
                        + " targetPages=" + target, e);
            }
        }
    }

    /**
     * 幂等确保已发布物理边界至少达到目标页数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在拿锁前拒绝空或非正最小容量。</li>
     *     <li>按 lifecycle shared→file-size 排他获取并确认 handle 打开，串行取得 current/target。</li>
     *     <li>target 不大于 current 时直接返回，不缩小文件，也不调用 gateway。</li>
     *     <li>target 更大时建立 {@code [current,target)}，完整成功后 volatile 发布 target；失败保留旧边界。</li>
     * </ol>
     *
     * <p>主要供 crash recovery 把 redo/page0 恢复出的逻辑大小与较短物理文件收敛。方法不 force：
     * recovery 编排在 replay/repair 完成后统一 {@code forceAll}；重复恢复可再次从已发布旧边界建立尾部。</p>
     *
     * @param minSizeInPages 非空正数最小发布物理页数
     * @throws DatabaseValidationException 目标为空、非正或 gateway 范围换算非法时抛出
     * @throws TablespaceNotOpenException handle 已关闭时抛出
     * @throws DataFilePhysicalException 新增范围建立失败时抛出；发布边界保持旧值
     */
    void ensureCapacity(PageNo minSizeInPages) {
        // 1. recovery/容量协调目标必须能保留至少一个正页范围。
        if (minSizeInPages == null || minSizeInPages.value() < 1) {
            throw new DatabaseValidationException("ensure capacity min size must be positive");
        }

        // 2. 与 autoExtend 共用锁序，防止基于陈旧 current 重复建立尾部。
        try (ResourceGuard s = lifecycleLatch.acquireShared(); ResourceGuard x = fileSizeLock.acquire()) {
            ensureOpen();
            long target = minSizeInPages.value();
            long current = currentSizeInPages;

            // 3. 已达到目标时保持幂等，不写已有页，也不发布新代次。
            if (target <= current) {
                return;
            }

            // 4. 新范围完成后才发布 target；本方法不执行 force。
            gateway.ensureAllocated(channel, current, target, pageSize, path);
            currentSizeInPages = target;
        }
    }

    /**
     * 幂等 drain 并关闭该 handle 拥有的 channel。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>获取 lifecycle exclusive，等待所有 shared 页 IO、扩展和 force owner 退出。</li>
     *     <li>已关闭时直接返回；否则先把 closed 单向置 true，阻止任何失败后的复用。</li>
     *     <li>关闭 channel；IOException 包装向上抛出，但 closed 不回滚，exclusive guard 始终释放。</li>
     * </ol>
     *
     * @throws DataFilePhysicalException channel close 失败时抛出；handle 仍保持 closed
     */
    @Override
    public void close() {
        // 1. exclusive 获取是 drain 点，成功时没有普通物理操作仍在使用 channel。
        try (ResourceGuard ignored = lifecycleLatch.acquireExclusive()) {
            // 2. 重复 close 幂等；首次 close 在实际 channel 调用前发布单向关闭状态。
            if (closed) {
                return;
            }
            closed = true;
            try {
                // 3. 关闭失败不恢复 handle 可用性，避免重用状态不明的 channel。
                channel.close();
            } catch (IOException e) {
                throw new DataFilePhysicalException("close data file failed: " + path, e);
            }
        }
    }

    /**
     * 校验页号位于当前发布边界，并换算为文件字节偏移。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空 PageNo；数值非负由值对象自身保证。</li>
     *     <li>读取 volatile size 快照，要求 {@code pageNo < size}；未发布尾部即使已经存在物理字节也越界。</li>
     *     <li>用 handle SpaceId/PageSize 构造 PageId 并执行溢出安全偏移换算。</li>
     * </ol>
     *
     * @param pageNo 待定位的非空表空间内页号
     * @return pageNo 对应的非负文件起始字节偏移
     * @throws DatabaseValidationException pageNo 为空或偏移换算非法时抛出
     * @throws PageOutOfBoundsException pageNo 大于等于当前发布 size 时抛出
     */
    private long boundedOffset(PageNo pageNo) {
        // 1. null 不具有物理页身份。
        if (pageNo == null) {
            throw new DatabaseValidationException("page no must not be null");
        }

        // 2. volatile 边界是普通 IO 的唯一可见范围，物理未发布尾部不可访问。
        long size = currentSizeInPages;
        if (pageNo.value() >= size) {
            throw new PageOutOfBoundsException("page out of bounds: space=" + spaceId.value()
                    + " pageNo=" + pageNo.value() + " size=" + size);
        }

        // 3. 统一复用 PageId 偏移公式，避免 fil 内出现另一套页几何。
        return PageId.of(spaceId, pageNo).offset(pageSize);
    }

    /**
     * 在已持有 lifecycle shared/exclusive 的前提下拒绝关闭 handle。
     *
     * <p>close 只能在 lifecycle exclusive 下置位；普通路径持 shared 调用本方法后，close 无法在本次
     * 操作结束前生效，因此不存在“检查通过后 channel 被并发关闭”的窗口。</p>
     *
     * @throws TablespaceNotOpenException closed 已发布时抛出
     */
    private void ensureOpen() {
        // 调用方已持 lifecycle guard；closed 为 true 后永不恢复。
        if (closed) {
            throw new TablespaceNotOpenException("data file handle closed: space=" + spaceId.value());
        }
    }

    /**
     * 要求 buffer 当前可读/可写剩余区间恰好为一个 handle page。
     *
     * <p>只检查 null 与 remaining，不修改 position/limit，也不判断只读属性；只读 dst 会由底层 read
     * 以运行时异常失败，src 则允许是只读 buffer。</p>
     *
     * @param buffer 读页目标或写页来源 buffer
     * @throws DatabaseValidationException buffer 为空或 remaining 不等于 pageSize 时抛出
     */
    private void requirePageSized(ByteBuffer buffer) {
        if (buffer == null) {
            throw new DatabaseValidationException("page buffer must not be null");
        }
        if (buffer.remaining() != pageSize.bytes()) {
            throw new DatabaseValidationException("page buffer remaining must equal page size: expected "
                    + pageSize.bytes() + " got " + buffer.remaining());
        }
    }

    /**
     * 从指定物理偏移开始，以 positional read 消耗 dst 全部 remaining。
     *
     * <p>成功后 dst position 到达原 limit；遇到 EOF 视为文件物理事实与已发布 size 不一致并抛异常。
     * IOException 保留为 cause。本循环依赖 FileChannel 最终取得进展，不改变 channel 共享 position。</p>
     *
     * @param dst 已校验为一页 remaining 的可写目标 buffer
     * @param offset 已通过边界/溢出校验的页起始字节偏移
     * @throws DataFilePhysicalException 读取失败或到达 EOF 时抛出
     */
    private void readFully(ByteBuffer dst, long offset) {
        long pos = offset;
        try {
            while (dst.hasRemaining()) {
                int n = channel.read(dst, pos);
                if (n < 0) {
                    throw new DataFilePhysicalException("unexpected EOF reading page at offset " + offset + " of " + path);
                }
                pos += n;
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("read page failed at offset " + offset + " of " + path, e);
        }
    }

    /**
     * 从指定物理偏移开始，以 positional write 消耗 src 全部 remaining。
     *
     * <p>成功后 src position 到达原 limit；方法不 force。IOException 可能发生在部分页写入后，
     * 调用方必须保留上层 dirty/recovery 状态。</p>
     *
     * @param src 已校验为一页 remaining 的源 buffer
     * @param offset 已通过边界/溢出校验的页起始字节偏移
     * @throws DataFilePhysicalException positional write 失败时抛出并保留 IO 根因
     */
    private void writeFully(ByteBuffer src, long offset) {
        long pos = offset;
        try {
            while (src.hasRemaining()) {
                pos += channel.write(src, pos);
            }
        } catch (IOException e) {
            throw new DataFilePhysicalException("write page failed at offset " + offset + " of " + path, e);
        }
    }

    /**
     * 校验 create/open 共同需要的文件 identity 与页几何引用。
     *
     * @param spaceId 非空表空间身份
     * @param path 非空文件路径；不检查存在性或规范化
     * @param pageSize 非空固定页大小
     * @throws DatabaseValidationException 任一参数为空时抛出
     */
    private static void validate(SpaceId spaceId, Path path, PageSize pageSize) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (path == null) {
            throw new DatabaseValidationException("data file path must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
    }

    /**
     * 尽力关闭尚未转交给 handle 的 channel。
     *
     * <p>仅用于 create/open 失败清理；空 channel 无操作。当前实现有意忽略 cleanup IOException，
     * 让调用路径保留原始创建/打开失败，但也不会把关闭失败附加为 suppressed。</p>
     *
     * @param channel 尚未转交的 channel，允许为空
     */
    private static void closeQuietly(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // cleanup 是 best-effort；方法契约明确当前不会覆盖或附加原始失败。
            }
        }
    }
}
