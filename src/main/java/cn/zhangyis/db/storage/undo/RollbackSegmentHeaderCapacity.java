package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * rollback segment page3 v3 定长目录的容量校验器。配置层与 repository 共用同一公式，避免一处接受的
 * slot/cache 组合在真正格式化 page3 时才发现越过 trailer。
 */
public final class RollbackSegmentHeaderCapacity {

    private RollbackSegmentHeaderCapacity() {
    }

    /**
     * 校验 active slot 与两个 per-kind cache 栈能同时放入实例页。
     *
     * @param pageSize 实例页大小。
     * @param slotCapacity active undo slot 数，必须为正。
     * @param cacheCapacityPerKind 每个 INSERT/UPDATE 栈的容量，0 表禁用。
     */
    public static void validate(PageSize pageSize, int slotCapacity, int cacheCapacityPerKind) {
        if (pageSize == null) {
            throw new DatabaseValidationException("rseg header capacity page size must not be null");
        }
        if (slotCapacity <= 0) {
            throw new DatabaseValidationException("rseg slot capacity must be positive: " + slotCapacity);
        }
        if (cacheCapacityPerKind < 0) {
            throw new DatabaseValidationException("undo cache capacity must not be negative: "
                    + cacheCapacityPerKind);
        }
        final long end;
        try {
            end = Math.addExact(RollbackSegmentHeaderLayout.SLOT_ARRAY_BASE,
                    Math.addExact(Math.multiplyExact((long) slotCapacity, Long.BYTES),
                            Math.multiplyExact(Math.multiplyExact((long) cacheCapacityPerKind, 2L), Long.BYTES)));
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("rseg page3 capacity calculation overflows", overflow);
        }
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        if (end > limit) {
            throw new DatabaseValidationException("rseg slot/cache capacity overflows page3: slots="
                    + slotCapacity + ", cachePerKind=" + cacheCapacityPerKind
                    + ", needs=" + end + ", limit=" + limit);
        }
    }
}
