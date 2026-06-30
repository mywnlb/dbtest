package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.10d BufferPoolRouter 单元测试：路由确定（同 PageId 恒同 index）、N=1 恒 0、范围合法、分布大致均匀、非法参数拒绝。
 * 路由是分片正确性的根基——所有 get/flush/invalidate 必须对同一 PageId 选同一 instance，否则帧会在错误分片被找/被删。
 */
class BufferPoolRouterTest {

    private static final SpaceId SPACE = SpaceId.of(7);

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    @Test
    void singleInstanceAlwaysRoutesToZero() {
        BufferPoolRouter router = new BufferPoolRouter(1);
        for (long no = 0; no < 100; no++) {
            assertEquals(0, router.route(page(no)), "N=1 时一切页恒路由到 instance 0");
        }
    }

    @Test
    void routeIsDeterministicAndInRange() {
        BufferPoolRouter router = new BufferPoolRouter(4);
        for (long no = 0; no < 200; no++) {
            int first = router.route(page(no));
            assertEquals(first, router.route(page(no)), "同一 PageId 必恒路由到同一 instance");
            assertTrue(first >= 0 && first < 4, "路由结果须落在 [0, instanceCount)：" + first);
        }
    }

    @Test
    void distributesAcrossInstancesReasonablyEvenly() {
        int instanceCount = 4;
        BufferPoolRouter router = new BufferPoolRouter(instanceCount);
        Map<Integer, Integer> counts = new HashMap<>();
        int total = 4000;
        for (long no = 0; no < total; no++) {
            counts.merge(router.route(page(no)), 1, Integer::sum);
        }
        assertEquals(instanceCount, counts.size(), "4 个 instance 都应分到页");
        int expected = total / instanceCount; // 1000
        for (Map.Entry<Integer, Integer> e : counts.entrySet()) {
            // 容忍 ±40% 的偏差：散列分布不要求精确均匀，只要不退化为某分片独占。
            assertTrue(e.getValue() >= expected * 0.6 && e.getValue() <= expected * 1.4,
                    "instance " + e.getKey() + " 分到 " + e.getValue() + " 页，偏离均匀过大");
        }
    }

    @Test
    void differentSpacesAlsoDistribute() {
        BufferPoolRouter router = new BufferPoolRouter(4);
        // 不同表空间的同页号不应全部撞到同一 instance（spaceId 必须参与散列）。
        Map<Integer, Integer> counts = new HashMap<>();
        for (int space = 1; space <= 64; space++) {
            counts.merge(router.route(PageId.of(SpaceId.of(space), PageNo.of(3))), 1, Integer::sum);
        }
        assertTrue(counts.size() >= 2, "spaceId 应参与路由散列，不能让同页号全撞一个 instance");
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(DatabaseValidationException.class, () -> new BufferPoolRouter(0));
        assertThrows(DatabaseValidationException.class, () -> new BufferPoolRouter(-1));
        assertThrows(DatabaseValidationException.class, () -> new BufferPoolRouter(4).route(null));
    }
}
