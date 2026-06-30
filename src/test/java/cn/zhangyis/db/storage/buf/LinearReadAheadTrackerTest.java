package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.10a linear read-ahead 检测器测试：同一 extent 内顺序访问达阈值 → 产出下一 extent 预取请求；
 * 乱序重置 run；同一 extent 不重复提交；顺序跨 extent 后对再下一 extent 重新触发。
 */
class LinearReadAheadTrackerTest {

    private static final SpaceId SPACE = SpaceId.of(1);

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    private static Optional<ReadAheadRequest> last(LinearReadAheadTracker t, long... pageNos) {
        Optional<ReadAheadRequest> result = Optional.empty();
        for (long no : pageNos) {
            result = t.record(page(no));
        }
        return result;
    }

    @Test
    void emitsNextExtentAfterThreshold() {
        LinearReadAheadTracker tracker = new LinearReadAheadTracker(4);

        assertTrue(tracker.record(page(0)).isEmpty());
        assertTrue(tracker.record(page(1)).isEmpty());
        assertTrue(tracker.record(page(2)).isEmpty());
        Optional<ReadAheadRequest> req = tracker.record(page(3)); // 第 4 个连续 → 达阈值

        assertTrue(req.isPresent());
        assertEquals(new ReadAheadRequest(SPACE, 64, LinearReadAheadTracker.PAGES_PER_EXTENT), req.get(),
                "extent 0 连续达阈值 → 预取 extent 1（起始页 64）");
    }

    @Test
    void resetsRunOnNonSequentialAccess() {
        LinearReadAheadTracker tracker = new LinearReadAheadTracker(4);

        tracker.record(page(0));
        tracker.record(page(1));
        tracker.record(page(2));
        assertTrue(tracker.record(page(5)).isEmpty(), "乱序跳变重置 run，不触发预取");
        // 重置后需重新累计：5,6,7,8 → 达阈值（均在 extent 0）
        tracker.record(page(6));
        tracker.record(page(7));
        assertTrue(tracker.record(page(8)).isPresent(), "重置后重新累计达阈值再触发");
    }

    @Test
    void doesNotResubmitSameNextExtent() {
        LinearReadAheadTracker tracker = new LinearReadAheadTracker(4);

        assertTrue(last(tracker, 0, 1, 2, 3).isPresent(), "首次达阈值发出 extent 1");
        assertTrue(tracker.record(page(4)).isEmpty(), "同一 extent 继续顺序访问不重复发 extent 1");
        assertTrue(tracker.record(page(5)).isEmpty());
    }

    @Test
    void sequentialCrossExtentReemitsForFurtherExtent() {
        LinearReadAheadTracker tracker = new LinearReadAheadTracker(4);
        // 在 extent 0 末尾顺序到 extent 1：60,61,62,63 达阈值发 extent 1；跨入 64 起新 run，64,65,66,67 发 extent 2。
        assertTrue(last(tracker, 60, 61, 62, 63).isPresent());
        Optional<ReadAheadRequest> req = last(tracker, 64, 65, 66, 67);
        assertTrue(req.isPresent());
        assertEquals(128, req.get().firstPageNo(), "跨入 extent 1 后顺序达阈值 → 预取 extent 2（起始页 128）");
    }

    @Test
    void rejectsInvalidThreshold() {
        assertThrows(DatabaseValidationException.class, () -> new LinearReadAheadTracker(0));
        assertThrows(DatabaseValidationException.class, () -> new LinearReadAheadTracker(65));
        assertThrows(DatabaseValidationException.class, () -> new LinearReadAheadTracker(4).record(null));
    }
}
