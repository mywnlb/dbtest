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
}
