package cn.zhangyis.db.storage.fsp;

/**
 * extent 取数策略（Strategy，设计 §7.2/§7.3）：给定 segment 已占 extent 数，返回本次应一次性获取的 extent 数（1..4）。
 */
public interface ExtentAllocationPolicy {

    /**
     * @param ownedExtentCount segment 当前已占 extent 数（三条 SEG 链长之和）。
     * @return 本次获取 extent 数，1..4。
     */
    int extentsToAcquire(long ownedExtentCount);
}
