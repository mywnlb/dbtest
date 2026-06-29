package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 追踪乱序完成的 redo LSN 区间，并只发布从当前边界开始连续覆盖的前缀。
 *
 * <p>InnoDB 的 recent written / recent closed 本质都是“区间可能乱序完成，但消费者只能读取连续边界”。
 * 本教学实现先用 {@link TreeMap} 保存少量 pending 区间，避免把 checkpoint 暴露给带 gap 的 LSN。
 * 调用方负责在外层持有自己的模块锁；本类型不自行加锁，便于被 redo manager 统一纳入锁顺序。
 */
final class ContiguousLsnTracker {

    /** 当前已连续完成的右开边界；消费者只能读取该值，不能越过 pending gap。 */
    private long contiguousEnd;

    /**
     * 尚未并入连续前缀的区间，key 为 start LSN，value 为 end LSN。相同 start 的重复完成会保留更大的 end。
     */
    private final NavigableMap<Long, Long> pending = new TreeMap<>();

    ContiguousLsnTracker(Lsn initialBoundary) {
        reset(initialBoundary);
    }

    /** 当前连续边界。 */
    Lsn boundary() {
        return Lsn.of(contiguousEnd);
    }

    /**
     * 记录一个已完成区间，并在前缀连续时推进边界。空区间和已经完全落在边界内的重复完成是 no-op。
     *
     * @param range 已完成的 redo 区间。
     * @return 推进后的连续边界。
     */
    Lsn mark(LogRange range) {
        if (range == null) {
            throw new DatabaseValidationException("tracked redo range must not be null");
        }
        long start = range.start().value();
        long end = range.end().value();
        if (start == end || end <= contiguousEnd) {
            return boundary();
        }
        if (start < contiguousEnd) {
            throw new DatabaseValidationException("tracked redo range overlaps committed boundary: "
                    + start + ".." + end + ", boundary=" + contiguousEnd);
        }
        Long previous = pending.get(start);
        if (previous == null || end > previous) {
            pending.put(start, end);
        }
        drainContiguousPrefix();
        return boundary();
    }

    /**
     * 恢复或重新初始化 tracker。恢复后的历史 redo 已由 recovery reader 验证完整，closed/flushed/current
     * 都从该边界继续，pending 区间必须清空。
     */
    void reset(Lsn boundary) {
        if (boundary == null) {
            throw new DatabaseValidationException("tracker boundary must not be null");
        }
        pending.clear();
        contiguousEnd = boundary.value();
    }

    private void drainContiguousPrefix() {
        while (!pending.isEmpty()) {
            var first = pending.firstEntry();
            if (first.getKey() != contiguousEnd) {
                return;
            }
            contiguousEnd = first.getValue();
            pending.pollFirstEntry();
        }
    }
}
