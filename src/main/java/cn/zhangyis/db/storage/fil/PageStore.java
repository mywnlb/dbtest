package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * 物理页 IO 门面（设计 §13.3 的物理读写部分 + §10）。按 PageId 定位文件偏移做 positional 整页读写，
 * 并提供 autoextend。它是纯物理视角：registry-无关、state-无关，不解析页内容、不算 checksum、不产 redo。
 *
 * <p>普通 IO 的 NORMAL/ACTIVE 逻辑准入由上层（storage.api/fsp）经 TablespaceRegistry.require 把关，不在本层。
 * pageSize/path 由编排方在 create/open 时注入。
 */
public interface PageStore extends AutoCloseable {

    /**
     * 创建表空间物理文件并登记句柄。文件必须不存在，按 initialSizeInPages 零填充。
     *
     * @param spaceId 表空间编号。
     * @param path 数据文件路径。
     * @param pageSize 页大小。
     * @param initialSizeInPages 初始页数。
     */
    void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages);

    /**
     * 打开已存在表空间物理文件并登记句柄。size 由文件长度推导，须整页对齐。
     *
     * @param spaceId 表空间编号。
     * @param path 数据文件路径。
     * @param pageSize 页大小。
     */
    void open(SpaceId spaceId, Path path, PageSize pageSize);

    /**
     * 读取整页到 dst。dst.remaining() 必须 == pageSize；未登记抛 TablespaceNotOpenException；越界抛 PageOutOfBoundsException。
     *
     * @param pageId 物理页定位键。
     * @param dst 目标缓冲。
     */
    void readPage(PageId pageId, ByteBuffer dst);

    /**
     * 写入整页。src.remaining() 必须 == pageSize。同页并发写串行化由上层 page latch 负责。
     *
     * @param pageId 物理页定位键。
     * @param src 源缓冲。
     */
    void writePage(PageId pageId, ByteBuffer src);

    /**
     * 对表空间执行一次自动扩展，返回扩展后的 currentSizeInPages。
     *
     * @param spaceId 表空间编号。
     * @return 扩展后的物理大小页数。
     */
    PageNo extend(SpaceId spaceId);

    /**
     * 查询当前物理大小页数。
     *
     * @param spaceId 表空间编号。
     * @return 当前物理大小页数。
     */
    PageNo currentSizeInPages(SpaceId spaceId);

    /**
     * 返回已登记表空间的数据文件路径。用于上层 metadata loader 在只有 SpaceId 时重建 TablespaceMetadata；
     * 物理 IO 仍通过已打开的 FileChannel 句柄完成，path 仅表达元数据来源。
     *
     * @param spaceId 表空间编号。
     * @return 已打开数据文件路径。
     */
    Path pathOf(SpaceId spaceId);

    /**
     * 对表空间数据文件执行 fsync/force。FlushCoordinator 在 data page write 后调用它，确保 data file 持久化边界明确。
     *
     * @param spaceId 表空间编号。
     */
    void force(SpaceId spaceId);

    /**
     * 对全部已打开表空间数据文件执行 fsync/force。crash recovery 在开放流量前调用它，把 replay/doublewrite repair/
     * file reconcile 写入的页一次性落盘，使「恢复写已持久」成立——此后即便 checkpoint 越过 recoveredToLsn 并回收
     * redo，再次崩溃也不会丢失恢复结果。
     */
    void forceAll();

    /**
     * 把单文件表空间物理缩短到目标页数并 force。该 API 只表达 fil 物理动作，不判断表空间类型、
     * undo inode 是否为空或恢复状态；上层生命周期服务必须先持独占 operation lease 并排空 Buffer Pool。
     *
     * @param spaceId 目标表空间。
     * @param targetSizeInPages 严格小于当前大小的正页数。
     */
    void truncate(SpaceId spaceId, PageNo targetSizeInPages);

    /**
     * 幂等地把单文件表空间物理大小扩到至少 {@code minSizeInPages}：已 >= 目标则 no-op，否则零填充新增页。
     * 它是 {@link #truncate} 的镜像但只增不减。供 crash recovery 把 redo 恢复出的 page0 权威逻辑大小重对齐到
     * 物理文件，弥补 {@code autoExtend} 不 fsync 在崩溃后留下的"物理短于逻辑"背离；不参与表空间类型/状态判断。
     *
     * @param spaceId 目标表空间。
     * @param minSizeInPages 期望的最小物理页数（正）。
     */
    void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages);

    /**
     * 关闭并注销单个表空间句柄。
     *
     * @param spaceId 表空间编号。
     */
    void close(SpaceId spaceId);

    /**
     * 关闭全部句柄。
     */
    @Override
    void close();
}
