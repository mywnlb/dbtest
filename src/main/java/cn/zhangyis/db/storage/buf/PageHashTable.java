package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 单个 {@code BufferPoolInstance} 的 page hash（§5.5）：{@link PageId} → {@link BufferFrame} 映射，外加按连续页区间
 * 统计驻留页数。命中的 frame 可能处于 CLEAN/DIRTY/FLUSHING/LOADING（LOADING 为载入占位）任一状态——本表只管「键是否
 * 在表」，状态语义由 instance 负责。
 *
 * <p><b>并发</b>：本类<b>不自带锁</b>。所属 {@code BufferPoolInstance} 的 pageHashLock 在外串行保护所有访问。
 * single-flight 载入占位（LOADING + {@code PageLoadFuture}）
 * 的编排留在 {@code BufferPoolInstance.acquire}，本表只提供 map 原语。
 */
final class PageHashTable {

    /** PageId → 驻留帧（含 LOADING 占位帧）。由 instance 锁在外保护。 */
    private final Map<PageId, BufferFrame> map = new HashMap<>();

    /** 命中返回帧，未命中返回 null。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code get} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    BufferFrame get(PageId pageId) {
        return map.get(pageId);
    }

    /** 注册/覆盖 pageId→frame 映射。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    void put(PageId pageId, BufferFrame frame) {
        map.put(pageId, frame);
    }

    /** 移除并返回该 pageId 的帧（不存在返回 null）。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code remove} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    BufferFrame remove(PageId pageId) {
        return map.remove(pageId);
    }

    /** 该 pageId 是否在表（含 LOADING 占位）。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code containsKey} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean containsKey(PageId pageId) {
        return map.containsKey(pageId);
    }

    /** 当前驻留帧数。 */
    int size() {
        return map.size();
    }

    /** 驻留帧集合（live view，调用方须在 instance 锁下使用或自行拷贝）。 */
    Collection<BufferFrame> values() {
        return map.values();
    }

    /** 驻留页键集合（live view，调用方须在 instance 锁下使用或自行拷贝）。 */
    Set<PageId> keySet() {
        return map.keySet();
    }

    /**
     * 统计某连续页区间 {@code [firstPageNo, firstPageNo+pageCount)} 内本表持有的页数。供 random read-ahead 判定
     * 「同一 extent 已有足够多页驻留」。逐页查表（O(pageCount)，与表大小无关）。
     *
     * @param spaceId     目标表空间。
     * @param firstPageNo 区间起始页号（含，≥0）。
     * @param pageCount   区间页数（≥1）。
     * @return 区间内本表持有的页数（0..pageCount）。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    int countInRange(SpaceId spaceId, long firstPageNo, int pageCount) {
        if (spaceId == null) {
            throw new DatabaseValidationException("page hash count space id must not be null");
        }
        if (firstPageNo < 0) {
            throw new DatabaseValidationException("page hash count firstPageNo must be >= 0: " + firstPageNo);
        }
        if (pageCount < 1) {
            throw new DatabaseValidationException("page hash count pageCount must be >= 1: " + pageCount);
        }
        int count = 0;
        for (long offset = 0; offset < pageCount; offset++) {
            if (map.containsKey(PageId.of(spaceId, PageNo.of(firstPageNo + offset)))) {
                count++;
            }
        }
        return count;
    }
}
