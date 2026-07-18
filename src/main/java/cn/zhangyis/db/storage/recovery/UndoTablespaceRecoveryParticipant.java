package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;

/**
 * undo TRUNCATING 恢复扩展点。实现必须只处理启动配置显式列出的 undo SpaceId：先修复/读取 page0 marker，
 * 再过滤 doublewrite 尾页；redo replay 完成后续作物理截断与 FSP 重建，返回前必须达到稳定状态。
 */
public interface UndoTablespaceRecoveryParticipant {

    /**
     * 在普通 doublewrite 页扫描前修复配置空间的 page0 并加载 marker。
     *
     * @param scanner 可选 scanner；为 null 时只能读取已完整的 page0。
     * @return 本阶段实际修复的 page0 数量。
     */
    int prepareDoublewrite(DoublewriteRecoveryScanner scanner);

    /** durable TRUNCATING target 之外的尾页必须返回 false，防止恢复已声明丢弃的旧字节。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code shouldRepairDoublewritePage} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean shouldRepairDoublewritePage(PageId pageId);

    /** redo replay 到完整边界后续作所有已发现的 TRUNCATING 空间。
     *
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     */
    void resumeAfterRedo(Lsn recoveredToLsn);
}
