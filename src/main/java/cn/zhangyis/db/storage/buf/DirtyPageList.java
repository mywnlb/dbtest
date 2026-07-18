package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Buffer Pool 分片内的真实 flush list。
 *
 * <p>本类只维护 dirty 页的定位键和 LSN 边界，不保存 {@link BufferFrame} 引用，也不读取页体。所属
 * {@link BufferPoolInstance} 必须在外持有 {@code flushListLock} 调用所有方法；本类自身不加锁，避免把锁顺序隐藏在
 * 容器内部。候选列表只是快照，flush 模块写盘前仍必须通过 {@link BufferPool#snapshotForFlush(PageId)} 重新确认页仍可刷。
 */
final class DirtyPageList {

    /** PageId -> 当前 flush list 条目；用于同页重复变脏时去重和更新 newest LSN。 */
    private final Map<PageId, DirtyPageCandidate> byPage = new LinkedHashMap<>();

    /** oldest LSN -> 同一 oldest 下按插入顺序排列的页；用于快速取 checkpoint/flush 的最老边界。 */
    private final TreeMap<Long, LinkedHashMap<PageId, DirtyPageCandidate>> byOldestLsn = new TreeMap<>();

    /**
     * 插入或更新一个 dirty 页条目。调用方应传入 frame 当前的 oldest/newest LSN；若同页已在链表中，先摘旧节点再按新的
     * oldest 重新挂入，从而避免重复候选。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param oldestModificationLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param newestModificationLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     */
    void upsert(PageId pageId, Lsn oldestModificationLsn, Lsn newestModificationLsn) {
        DirtyPageCandidate candidate = new DirtyPageCandidate(pageId, oldestModificationLsn, newestModificationLsn);
        DirtyPageCandidate previous = byPage.put(pageId, candidate);
        if (previous != null) {
            removeFromOldestBucket(previous);
        }
        byOldestLsn.computeIfAbsent(oldestModificationLsn.value(), ignored -> new LinkedHashMap<>())
                .put(pageId, candidate);
    }

    /** 从 flush list 移除一个页；页已 clean、FREE、stale 或从 page hash 摘除时调用。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void remove(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("dirty page id must not be null");
        }
        DirtyPageCandidate previous = byPage.remove(pageId);
        if (previous != null) {
            removeFromOldestBucket(previous);
        }
    }

    /**
     * 返回 oldest <= targetLsn 的候选快照，按 oldest LSN 升序排列。maxPages 为 0 时只做参数校验并返回空列表。
     *
     * @param targetLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param maxPages 参与 {@code candidatesUpTo} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    List<DirtyPageCandidate> candidatesUpTo(Lsn targetLsn, int maxPages) {
        if (targetLsn == null) {
            throw new DatabaseValidationException("dirty target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("dirty max pages must not be negative: " + maxPages);
        }
        List<DirtyPageCandidate> result = new ArrayList<>();
        if (maxPages == 0) {
            return List.of();
        }
        for (Map.Entry<Long, LinkedHashMap<PageId, DirtyPageCandidate>> bucket : byOldestLsn.entrySet()) {
            if (bucket.getKey() > targetLsn.value()) {
                break;
            }
            for (DirtyPageCandidate candidate : bucket.getValue().values()) {
                result.add(candidate);
                if (result.size() == maxPages) {
                    return List.copyOf(result);
                }
            }
        }
        return List.copyOf(result);
    }

    /** 返回当前最老 dirty LSN；没有 dirty 页时返回 null。FLUSHING 页仍保留在本链表中，所以仍约束 checkpoint。
     *
     * @return {@code oldestDirtyLsnOrNull} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code Lsn} 结果
     */
    Lsn oldestDirtyLsnOrNull() {
        if (byOldestLsn.isEmpty()) {
            return null;
        }
        return Lsn.of(byOldestLsn.firstKey());
    }

    /** 当前分片是否存在 dirty 或 FLUSHING 页。 */
    boolean hasDirtyPages() {
        return !byPage.isEmpty();
    }

    private void removeFromOldestBucket(DirtyPageCandidate candidate) {
        LinkedHashMap<PageId, DirtyPageCandidate> bucket = byOldestLsn.get(candidate.oldestModificationLsn().value());
        if (bucket == null) {
            return;
        }
        bucket.remove(candidate.pageId());
        if (bucket.isEmpty()) {
            byOldestLsn.remove(candidate.oldestModificationLsn().value());
        }
    }
}
