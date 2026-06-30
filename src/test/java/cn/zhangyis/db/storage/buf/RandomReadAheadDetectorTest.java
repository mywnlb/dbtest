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
 * 0.10c random read-ahead 检测器测试：同一 extent 驻留页数达阈值 → 产出整 extent 预取请求；不足阈值不触发；
 * 同一 extent 在 bounded recent 窗内去重；窗口前移后同一 extent 可再次触发（不做永久 set，避免淘汰后永不再触发）。
 */
class RandomReadAheadDetectorTest {

    private static final SpaceId SPACE = SpaceId.of(1);
    private static final int EXTENT = RandomReadAheadDetector.PAGES_PER_EXTENT; // 64

    /** 取 extent e 内偏移 offset 的页。 */
    private static PageId page(long extent, long offset) {
        return PageId.of(SPACE, PageNo.of(extent * EXTENT + offset));
    }

    @Test
    void randomDetectorEmitsExtentWhenEnoughResident() {
        RandomReadAheadDetector detector = new RandomReadAheadDetector(4);

        // 访问 extent 1 内某页，且该 extent 已驻留 4 页（达阈值）→ 预取整个 extent 1（页 64..127）。
        Optional<ReadAheadRequest> req = detector.record(page(1, 6), 4);

        assertTrue(req.isPresent(), "extent 驻留数达阈值应产出预取请求");
        assertEquals(new ReadAheadRequest(SPACE, 64, EXTENT), req.get(),
                "命中 → 预取被访问页所在整 extent（起始页 64、整 extent 64 页）");
    }

    @Test
    void belowThresholdNoEmit() {
        RandomReadAheadDetector detector = new RandomReadAheadDetector(4);

        assertTrue(detector.record(page(1, 6), 3).isEmpty(), "驻留数不足阈值不触发预取");
    }

    @Test
    void dedupSameExtent() {
        RandomReadAheadDetector detector = new RandomReadAheadDetector(4);

        assertTrue(detector.record(page(1, 6), 4).isPresent(), "首次达阈值发出 extent 1 预取");
        assertTrue(detector.record(page(1, 20), 5).isEmpty(),
                "同一 extent 仍在 recent 窗内：即便再次达阈值也不重复提交");
    }

    @Test
    void canEmitAgainAfterRecentWindowMovesOn() {
        // recent 窗容量 2：发出 extent 0、1、2 后，extent 0 被挤出窗口，可再次触发。
        RandomReadAheadDetector detector = new RandomReadAheadDetector(4, 2);

        assertTrue(detector.record(page(0, 1), 4).isPresent(), "发出 extent 0");
        assertTrue(detector.record(page(1, 1), 4).isPresent(), "发出 extent 1");
        assertTrue(detector.record(page(2, 1), 4).isPresent(), "发出 extent 2，挤出 extent 0");
        assertTrue(detector.record(page(0, 1), 4).isPresent(),
                "extent 0 已移出 recent 窗：同一 extent 可再次触发（bounded 去重而非永久 set）");
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(DatabaseValidationException.class, () -> new RandomReadAheadDetector(0));
        assertThrows(DatabaseValidationException.class, () -> new RandomReadAheadDetector(65));
        assertThrows(DatabaseValidationException.class, () -> new RandomReadAheadDetector(4, 0));
        assertThrows(DatabaseValidationException.class, () -> new RandomReadAheadDetector(4).record(null, 4));
        assertThrows(DatabaseValidationException.class, () -> new RandomReadAheadDetector(4).record(page(1, 0), -1));
    }
}
