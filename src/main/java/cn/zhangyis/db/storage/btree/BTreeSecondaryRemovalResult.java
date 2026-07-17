package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.List;

/**
 * 二级索引 entry 的物理删除结果。删除可能触发 leaf merge 或原地 root shrink，因此必须把结构变化后的
 * index 快照与已释放页带回编排层；ABSENT/STATE_CONFLICT 时两者保持原样。
 *
 * @param status     物理删除状态；不能为 null。
 * @param indexAfter 操作后的索引快照；root shrink 时 level 会降低，不能为 null。
 * @param freedPages merge/root shrink 归还给 FSP 的页；不可变且不能为 null。
 */
public record BTreeSecondaryRemovalResult(SecondaryEntryRemovalStatus status,
                                          BTreeIndex indexAfter,
                                          List<PageId> freedPages) {

    /**
     * 校验并冻结二级 entry 物理删除结果。
     *
     * @param status     删除状态，不能为 {@code null}。
     * @param indexAfter 操作完成后的索引结构快照，不能为 {@code null}。
     * @param freedPages 本次 merge/root-shrink 释放的页；构造时复制为不可变列表。
     * @throws DatabaseValidationException 任一结果字段缺失时抛出。
     */
    public BTreeSecondaryRemovalResult {
        if (status == null || indexAfter == null || freedPages == null) {
            throw new DatabaseValidationException("secondary removal result fields must not be null");
        }
        freedPages = List.copyOf(freedPages);
    }

    /**
     * 构造未修改任何页的删除结果。
     *
     * @param status 未修改原因，只能是 {@link SecondaryEntryRemovalStatus#ABSENT} 或
     *               {@link SecondaryEntryRemovalStatus#STATE_CONFLICT}。
     * @param index  调用前索引快照；由于未改页，原样作为 {@code indexAfter} 返回。
     * @return 不含释放页的不可变结果。
     * @throws DatabaseValidationException {@code status} 错误地声明为 REMOVED，或其它字段无效时抛出。
     */
    public static BTreeSecondaryRemovalResult noChange(SecondaryEntryRemovalStatus status, BTreeIndex index) {
        if (status == SecondaryEntryRemovalStatus.REMOVED) {
            throw new DatabaseValidationException("removed secondary result requires structural outcome");
        }
        return new BTreeSecondaryRemovalResult(status, index, List.of());
    }

    /**
     * 构造成功物理摘除结果。
     *
     * @param indexAfter entry 删除及潜在 merge/root-shrink 完成后的索引快照。
     * @param freedPages 已从树结构摘除、可交还 FSP 的页集合。
     * @return 状态为 REMOVED 且携带结构后置状态的不可变结果。
     * @throws DatabaseValidationException 任一参数为空时抛出。
     */
    public static BTreeSecondaryRemovalResult removed(BTreeIndex indexAfter, List<PageId> freedPages) {
        return new BTreeSecondaryRemovalResult(SecondaryEntryRemovalStatus.REMOVED, indexAfter, freedPages);
    }
}
