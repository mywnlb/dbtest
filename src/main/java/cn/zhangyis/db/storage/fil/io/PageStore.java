package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.PageOutOfBoundsException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;


import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * 单文件表空间的物理整页 IO 与文件尺寸门面。
 *
 * <p>实现按 {@link PageId} 的 {@code pageNo * pageSize} 做 positional 整页读写，并管理 create/open、
 * 一次 autoextend、recovery ensure-capacity、truncate、force 和 handle close。它保持纯物理视角：
 * 不解析 page envelope/body，不验证 checksum，不生成 redo，也不判断 FSP allocation。</p>
 *
 * <p>普通操作的 {@code NORMAL/ACTIVE} 准入由上层在 access lease 内通过
 * {@link TablespaceRegistry#require(SpaceId)} 完成；本接口故意不依赖 {@link TablespaceMetadata} 或
 * registry 状态。写页前的 WAL/doublewrite/checkpoint 顺序同样由 flush/MTR/recovery 调用方保证，
 * {@link #writePage(PageId, ByteBuffer)} 本身不会阻止未 durable redo 对应的页写入。</p>
 *
 * <p>所有 size API 表达物理 handle 的页数，不等同于 page0 的逻辑 free-limit。create/extend/ensure-capacity
 * 建立新范围后不会隐式 force；需要 durable 边界的流程必须显式调用 force。</p>
 */
public interface PageStore extends AutoCloseable {

    /**
     * 创建一个不存在的数据文件，初始化指定页数后登记物理 handle。
     *
     * <p>默认 gateway 将 {@code [0,initialSizeInPages)} 完整零填充。成功只表示 handle 已发布，
     * 不解析/初始化 page0 FSP 内容，也不保证初始范围已经 force。</p>
     *
     * @param spaceId 非空表空间物理标识；同一 PageStore 实例中必须尚未登记
     * @param path 非空目标路径；文件必须不存在，父目录必须可由文件系统访问
     * @param pageSize 非空固定页大小，绑定 handle 整个生命周期
     * @param initialSizeInPages 非空非负初始页数；零会创建零长度文件，但无法读取 page0
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法或 SpaceId 已登记时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 文件已存在、创建或零填充失败时抛出
     */
    void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages);

    /**
     * 打开已有单个数据文件，并按文件长度恢复物理页数后登记 handle。
     *
     * <p>该方法只校验长度能被 pageSize 整除，不读取 page0 identity、type、checksum 或 lifecycle。</p>
     *
     * @param spaceId 非空表空间物理标识；同一 PageStore 实例中必须尚未登记
     * @param path 非空已有数据文件路径
     * @param pageSize 非空固定页大小，必须与文件真实格式一致
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法或 SpaceId 已登记时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 文件不存在或无法打开时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFileCorruptedException 文件长度不按 pageSize 对齐时抛出
     */
    void open(SpaceId spaceId, Path path, PageSize pageSize);

    /**
     * 从目标页物理偏移读取恰好一个完整页到 {@code dst.remaining()}。
     *
     * <p>成功会把 dst position 推进一个 pageSize，不自动 flip；不校验读取页内容。未登记/已关闭与
     * 越界分别给出明确领域异常。</p>
     *
     * @param pageId 非空物理页定位键，pageNo 必须小于当前 handle 页数
     * @param dst 非空可写目标 buffer，其 remaining 必须精确等于该 handle 的 pageSize
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数或 buffer 尺寸非法时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记或 handle 已关闭时抛出
     * @throws PageOutOfBoundsException pageNo 不在当前物理范围内时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 读取失败或意外 EOF 时抛出
     */
    void readPage(PageId pageId, ByteBuffer dst);

    /**
     * 把 {@code src.remaining()} 的一个完整页写到目标物理偏移。
     *
     * <p>成功会把 src position 推进一个 pageSize，不自动 rewind。fil 不串行同页写、不执行 WAL gate
     * 或 force；上层 page latch/flush/recovery 必须保证镜像稳定和写入顺序。</p>
     *
     * @param pageId 非空物理页定位键，pageNo 必须小于当前 handle 页数
     * @param src 非空源 buffer，其 remaining 必须精确等于该 handle 的 pageSize
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数或 buffer 尺寸非法时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记或 handle 已关闭时抛出
     * @throws PageOutOfBoundsException pageNo 不在当前物理范围内时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException positional write 失败时抛出
     */
    void writePage(PageId pageId, ByteBuffer src);

    /**
     * 按 PageStore 配置的策略执行一次物理文件尾扩展。
     *
     * <p>新增页在完整初始化后才发布到 handle 的 volatile size；失败时保留旧可见大小，可能留下不可见
     * 尾部供后续重试。方法不更新 page0/FSP/registry，也不 force。</p>
     *
     * @param spaceId 已登记的非空表空间标识
     * @return 本次正增量发布后的物理页数
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException SpaceId 为空、策略返回非法增量或
     *         尺寸计算非法时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记或 handle 已关闭时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 新增范围建立失败时抛出
     */
    PageNo extend(SpaceId spaceId);

    /**
     * 查询 handle 当前发布的 volatile 物理页数。
     *
     * @param spaceId 已登记的非空表空间标识
     * @return 调用时观察到的物理页数快照；返回后可被并发 extend 增大
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记时抛出
     */
    PageNo currentSizeInPages(SpaceId spaceId);

    /**
     * 返回已登记 handle 创建/打开时绑定的数据文件路径。
     *
     * <p>当前 page0 metadata loader 用该值重建 {@link TablespaceMetadata} 文件描述；返回路径不授予
     * 额外文件 ownership，物理 IO 仍通过 PageStore handle。</p>
     *
     * @param spaceId 已登记的非空表空间标识
     * @return handle 生命周期内不变的路径值
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记时抛出
     */
    Path pathOf(SpaceId spaceId);

    /**
     * 对一个已登记数据文件执行串行化的 {@code FileChannel.force(true)}。
     *
     * <p>flush/recovery 调用方负责在进入前满足 WAL 或恢复阶段约束；本方法只建立数据文件 durable 边界。</p>
     *
     * @param spaceId 已登记的非空表空间标识
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记或 handle 已关闭时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException force 失败时抛出
     */
    void force(SpaceId spaceId);

    /**
     * 尝试对调用期间快照中的全部已登记数据文件执行 force。
     *
     * <p>当前实现用于 crash recovery 在开放流量前固化 replay/doublewrite repair/file reconcile 结果。
     * 单文件失败不会阻止继续尝试其它快照项，结束时抛聚合异常并把其余失败作为 suppressed cause。
     * 并发新打开的 handle 不保证包含在本次快照中。</p>
     *
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 至少一个文件 force 失败时抛出聚合异常
     */
    void forceAll();

    /**
     * 把单文件物理缩短到严格更小的正数页边界，并立即 force。
     *
     * <p>fil 不判断表空间类型、undo inode/history 是否为空或 durable marker 状态；上层必须先持同空间
     * 独占 operation lease、flush 并失效 Buffer Pool。truncate 物理成功后，即使后续 force 失败，handle
     * 也保留收紧后的 size，防止访问已不存在的尾部。</p>
     *
     * @param spaceId 已登记的非空目标表空间标识
     * @param targetSizeInPages 非空、严格小于当前物理大小的正页数
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法、目标非正或不小于当前大小时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记或 handle 已关闭时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException truncate 或 force 失败时抛出
     */
    void truncate(SpaceId spaceId, PageNo targetSizeInPages);

    /**
     * 幂等地把单文件物理大小扩到至少 {@code minSizeInPages}。
     *
     * <p>当前大小已达目标时 no-op，否则完整零填充新增尾部再发布 size。主要供 crash recovery 把 redo/page0
     * 逻辑大小与可能较短的物理文件重对齐；方法不判断类型/状态、不更新 FSP，也不 force。</p>
     *
     * @param spaceId 已登记的非空目标表空间标识
     * @param minSizeInPages 非空正数最小物理页数
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法或目标非正时抛出
     * @throws TablespaceNotOpenException SpaceId 未登记或 handle 已关闭时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 新增范围建立失败时抛出
     */
    void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages);

    /**
     * 注销并关闭单个物理 handle。
     *
     * <p>调用应由外层生命周期协议阻止新业务访问。当前实现先 remove 路由再 drain/close；
     * 未登记时幂等 no-op。</p>
     *
     * @param spaceId 待关闭的非空表空间标识
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException channel close 失败时抛出
     */
    void close(SpaceId spaceId);

    /**
     * 注销当前全部 handle，并逐个尝试关闭 channel。
     *
     * <p>单文件失败不阻止其它 handle cleanup；结束时抛聚合异常。并发 create/open 的外层关闭顺序
     * 必须由组合根保证，本接口不提供全局 lifecycle gate。</p>
     *
     * @throws cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException 至少一个 handle 关闭失败时抛出聚合异常
     */
    @Override
    void close();
}
