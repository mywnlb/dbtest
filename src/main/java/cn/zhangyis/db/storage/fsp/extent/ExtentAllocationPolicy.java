package cn.zhangyis.db.storage.fsp.extent;

/**
 * extent 取数策略（Strategy，设计 §7.2/§7.3）：给定 segment、方向 hint 和本次页需求，返回本次应一次性获取的
 * extent 数（1..4）。策略只决定“取多少”，具体从 free-list 选择哪个 extent 由 {@link FreeExtentService} 完成。
 */
public interface ExtentAllocationPolicy {

    /**
     * @param request 分配上下文，包含 segment purpose、方向、hint、当前 segment 大小和页大小。
     * @return 本次获取 extent 数，1..4。
     */
    int extentsToAcquire(ExtentAllocationRequest request);
}
