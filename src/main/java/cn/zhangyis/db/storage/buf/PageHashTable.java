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
 * <p><b>并发</b>：本类<b>不自带锁</b>。所属 {@code BufferPoolInstance} 的 {@code instanceLock} 在外串行保护所有访问
 * （本片采用「单 instance 锁」，不内置 §13.1 的独立 pageHashLock）。single-flight 载入占位（LOADING + {@code PageLoadFuture}）
 * 的编排留在 {@code BufferPoolInstance.acquire}，本表只提供 map 原语。
 */
final class PageHashTable {

    /** PageId → 驻留帧（含 LOADING 占位帧）。由 instance 锁在外保护。 */
    private final Map<PageId, BufferFrame> map = new HashMap<>();

    /** 命中返回帧，未命中返回 null。 */
    BufferFrame get(PageId pageId) {
        return map.get(pageId);
    }

    /** 注册/覆盖 pageId→frame 映射。 */
    void put(PageId pageId, BufferFrame frame) {
        map.put(pageId, frame);
    }

    /** 移除并返回该 pageId 的帧（不存在返回 null）。 */
    BufferFrame remove(PageId pageId) {
        return map.remove(pageId);
    }

    /** 该 pageId 是否在表（含 LOADING 占位）。 */
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
