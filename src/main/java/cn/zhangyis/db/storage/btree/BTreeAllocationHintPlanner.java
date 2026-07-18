package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.PageAllocationHint;
import cn.zhangyis.db.storage.record.page.SearchKey;

/**
 * B+Tree split 页分配 hint 规划器。它只做保守边界判定：插入 key 必须落在当前 leaf 的最左或最右边界之外，
 * 且对应方向没有 sibling，才把本次 split 视为明显顺序增长；其它情况返回 {@link PageAllocationHint#none()}。
 *
 * <p>该判断不依赖 record 层 PAGE_DIRECTION。当前 record 插入器的方向字段仍是教学简化，尚不能代表真实 workload
 * 方向；这里使用 B+Tree split 时已经掌握的 key 边界和 FIL sibling 状态，避免把随机中间 split 误导到批量 extent 增长。
 */
final class BTreeAllocationHintPlanner {

    private BTreeAllocationHintPlanner() {
    }

    /**
     * 为 leaf split 生成页分配 hint。返回值只影响 DiskSpaceManager 需要新 extent 时的选择策略，不改变 split 点、
     * sibling 链或 parent separator。
     *
     * @param leafId 正在 split 的 leaf 页。
     * @param insertedKey 待插入 key。
     * @param minKey 当前 leaf 现有最小 key。
     * @param maxKey 当前 leaf 现有最大 key。
     * @param hasLeftSibling 当前 leaf 是否已有左 sibling。
     * @param hasRightSibling 当前 leaf 是否已有右 sibling。
     * @param pagesNeeded 本次 leaf split 预计分配的新 leaf 页数。
     * @param index 索引元数据。
     * @param comparator SearchKey 比较器。
     * @return 保守方向 hint 或 none。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static PageAllocationHint leafSplitHint(PageId leafId, SearchKey insertedKey, SearchKey minKey, SearchKey maxKey,
                                            boolean hasLeftSibling, boolean hasRightSibling, long pagesNeeded,
                                            BTreeIndex index, SearchKeyComparator comparator) {
        if (leafId == null || insertedKey == null || minKey == null || maxKey == null
                || index == null || comparator == null) {
            throw new DatabaseValidationException("btree allocation hint args must not be null");
        }
        int aboveMax = comparator.compare(insertedKey, maxKey, index.keyDef(), index.schema());
        if (aboveMax > 0 && !hasRightSibling) {
            return PageAllocationHint.up(leafId.pageNo(), pagesNeeded);
        }
        int belowMin = comparator.compare(insertedKey, minKey, index.keyDef(), index.schema());
        if (belowMin < 0 && !hasLeftSibling) {
            return PageAllocationHint.down(leafId.pageNo(), pagesNeeded);
        }
        return PageAllocationHint.none();
    }
}
