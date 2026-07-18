package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.List;

/**
 * 聚簇记录物理删除 / purge 结果（T1.3d；0.12 起携带结构变更后果）。{@code deleteClustered} /
 * {@code purgeDeleteMarkedClustered} 是幂等的：未命中、所有权（DB_TRX_ID/DB_ROLL_PTR）不匹配都返回
 * {@code removed=false} 而非抛异常，使 rollback 反向走链 / purge 在 orphan undo / 重试 / stale 场景下安全收敛。
 *
 * <p><b>0.12 merge/root shrink</b>：物理删除可能使叶/内部页低于填充阈值而触发 merge 与原地 root shrink。
 * 这些操作改变树高（root shrink 降 {@code rootLevel}）并回收页，因此结果对称 {@link BTreeInsertResult} 额外携带：
 * <ul>
 *   <li>{@code indexAfter}：操作后应使用的索引快照。root shrink 会降低 {@code rootLevel}（root 页号稳定），
 *       调用方（如批量 rollback / purge 持同一快照）必须改用本字段，否则后续操作的 {@code rootLevel} 会陈旧。
 *       未发生 shrink 时等于入参 index。</li>
 *   <li>{@code freedPages}：本次操作归还给 Disk Manager 的页（merge victim / root shrink 吸收掉的 child），供观测；
 *       无回收时为空列表。</li>
 * </ul>
 * 注意：导航本身按页实际 level 下降（见 {@code openRoot} 的高度权威说明），{@code indexAfter} 主要用于保持调用方
 * 快照一致与观测，并非导航正确性的硬依赖。
 *
 * @param removed    是否真正摘除了一条匹配记录；{@code false} 表示「未命中或非本 undo/版本的行，未做任何修改」。
 * @param indexAfter 操作后的索引快照；未变则等于入参 index，不能为 null。
 * @param freedPages 本次回收的页（不可变快照），不能为 null（可为空）。
 */
public record BTreeDeleteResult(boolean removed, BTreeIndex indexAfter, List<PageId> freedPages) {

    public BTreeDeleteResult {
        if (indexAfter == null) {
            throw new DatabaseValidationException("BTreeDeleteResult indexAfter must not be null");
        }
        if (freedPages == null) {
            throw new DatabaseValidationException("BTreeDeleteResult freedPages must not be null");
        }
        freedPages = List.copyOf(freedPages);
    }

    /** 幂等 no-op（未命中 / 所有权不匹配 / stale）：未删、索引不变、无回收。
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code noChange} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static BTreeDeleteResult noChange(BTreeIndex index) {
        return new BTreeDeleteResult(false, index, List.of());
    }

    /** 成功删除：携带操作后索引快照（可能因 root shrink 降级）与回收页（可能为空，表示未触发 merge）。
     *
     * @param indexAfter 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param freedPages 参与 {@code removed} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code removed} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public static BTreeDeleteResult removed(BTreeIndex indexAfter, List<PageId> freedPages) {
        return new BTreeDeleteResult(true, indexAfter, freedPages);
    }
}
