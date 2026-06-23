package cn.zhangyis.db.storage.fsp.extent;

/**
 * 默认 extent 取数策略：顺序增长、封顶 4（设计 §7.2 step3 大 segment 一次最多 4 extent）。
 * ownedExtentCount<=0→1；否则 min(4, ownedExtentCount)。
 */
public final class DefaultExtentAllocationPolicy implements ExtentAllocationPolicy {

    @Override
    public int extentsToAcquire(long ownedExtentCount) {
        if (ownedExtentCount <= 0) {
            return 1;
        }
        return (int) Math.min(4L, ownedExtentCount);
    }
}
