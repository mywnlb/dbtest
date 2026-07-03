package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * undo 页分配端口。接口放在 undo 模块内，只使用 domain 值对象、MTR 与 undo 自有 handle，不暴露
 * {@code storage.api.DiskSpaceManager}/{@code storage.api.SegmentRef}，从依赖方向上阻止 undo 反向依赖 api。
 *
 * <p>实现可以透传 FSP 的空间不足异常；undo 层不重新包装，因为空间耗尽属于底层分配结果，而不是 undo
 * record 格式损坏。返回页均为裸分配页，调用方必须再经 {@link UndoPageAccess} 格式化为 UNDO 页。
 */
public interface UndoSpaceAllocator {

    /**
     * 建立一个 UNDO segment 并分配首页。返回 handle 的 first/last 页相同，页体仍是 ALLOCATED 状态，
     * 后续由 undo 页访问器在同一 MTR 中重初始化为 UNDO first page。
     *
     * @param mtr       当前物理短事务，承载 FSP 元页修改和分配页初始化。
     * @param undoSpace undo 表空间。
     * @return 新建 undo segment 的定位 handle。
     */
    UndoSegmentHandle createUndoSegment(MiniTransaction mtr, SpaceId undoSpace);

    /**
     * 为既有 undo segment 的页链增长预留若干页容量。调用方必须在任何页链修改前调用该方法，并在 grow 成功、
     * 失败或 MTR 结束时关闭返回 guard；底层实现可把该请求映射为 UNDO 类型的表空间 reservation。
     *
     * @param mtr 当前物理短事务。
     * @param undoSpace undo 表空间。
     * @param pages 本次 grow 最多会创建的 undo 页数。
     * @return undo 模块自有的预留 guard。
     */
    UndoSpaceReservation reserveGrowPages(MiniTransaction mtr, SpaceId undoSpace, long pages);

    /**
     * 在既有 undo segment 内续分配一页，供 append 溢出生长页链使用。方法不格式化页、不写 FIL 链；
     * 调用方必须先完成单条记录容量 preflight，避免 MTR 无 content undo 时留下半生长脏链。
     *
     * @param mtr       当前物理短事务。
     * @param undoSpace undo 表空间。
     * @param inodeSlot segment inode 槽下标。
     * @param segmentId segment 逻辑编号。
     * @return 新分配的裸页。
     */
    PageId allocatePage(MiniTransaction mtr, SpaceId undoSpace, int inodeSlot, SegmentId segmentId);

    /**
     * 回收（drop）一个 UNDO segment：归还其全部 fragment 页/extent 给 FSP free list 并清空 inode 槽。purge 在某已提交
     * undo log 处理完毕、或纯 insert undo 提交后调用，物理回收 undo 段空间。只用 undo 自有 {@link UndoSegmentHandle}
     * （提供 inodeSlot/segmentId），不暴露 {@code storage.api.SegmentRef}，维持 undo → api 端口的依赖方向。
     *
     * @param mtr    当前物理短事务，承载 FSP 元页（page0/page2/XDES）修改。
     * @param handle 待回收 undo segment 的定位 handle。
     */
    void dropUndoSegment(MiniTransaction mtr, UndoSegmentHandle handle);
}
