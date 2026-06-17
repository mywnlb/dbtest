package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * LruReplacementPolicy 测试固定 LRU 顺序：插入序即初始淘汰序，访问把帧移到 MRU，移除剔出。
 */
class LruReplacementPolicyTest {

    private static final PageSize PS = PageSize.ofBytes(4 * 1024);

    @Test
    void victimOrderShouldFollowLruThenMru() {
        LruReplacementPolicy policy = new LruReplacementPolicy();
        BufferFrame a = new BufferFrame(PS);
        BufferFrame b = new BufferFrame(PS);
        BufferFrame c = new BufferFrame(PS);

        policy.onInsert(a);
        policy.onInsert(b);
        policy.onInsert(c);
        assertEquals(List.of(a, b, c), drain(policy));

        policy.onAccess(a);
        assertEquals(List.of(b, c, a), drain(policy));

        policy.onRemove(b);
        assertEquals(List.of(c, a), drain(policy));
    }

    private static List<BufferFrame> drain(LruReplacementPolicy policy) {
        List<BufferFrame> out = new ArrayList<>();
        for (BufferFrame f : policy.victimOrder()) {
            out.add(f);
        }
        return out;
    }
}
