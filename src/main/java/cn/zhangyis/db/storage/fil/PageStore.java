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
     * 对表空间数据文件执行 fsync/force。FlushCoordinator 在 data page write 后调用它，确保 data file 持久化边界明确。
     *
     * @param spaceId 表空间编号。
     */
    void force(SpaceId spaceId);

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
