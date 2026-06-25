package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Midpoint LRU 策略测试（Phase A，设计 §6.1/§6.4）。用可控时钟驱动 {@code oldBlocksTime} 提升窗，
 * 验证：读入即进 old 子链、窗内重访不提升（扫描抗污染）、过窗重访升 new、近 new 头重访不抖动。
 *
 * <p>约定：{@code victimOrder()} 迭代序 = 淘汰优先序（最先被淘汰在前）；old 子链整体比 new 子链更易淘汰。
 */
class MidpointLruReplacementPolicyTest {

    private static final PageSize PS = PageSize.ofBytes(4 * 1024);
    /** 设计默认提升窗，测试用毫秒域可控时钟跨越它。 */
    private static final long OLD_BLOCKS_TIME_MS = 1000L;

    /** 可控毫秒时钟：测试显式推进，避免依赖墙钟造成不确定性。 */
    private final AtomicLong clock = new AtomicLong(0);

    private MidpointLruReplacementPolicy newPolicy() {
        return new MidpointLruReplacementPolicy(clock::get);
    }

    /**
     * 扫描抗污染原语：页读入后在 {@code oldBlocksTime} 窗内被再次访问，不得提升到 new 子链，
     * 且在 old 子链中保持原位（与 plain LRU 的"访问即移到 MRU"形成对比）。
     */
    @Test
    void accessWithinOldWindowDoesNotPromoteNorReorder() {
        MidpointLruReplacementPolicy policy = newPolicy();
        BufferFrame a = new BufferFrame(PS);
        BufferFrame b = new BufferFrame(PS);
        BufferFrame c = new BufferFrame(PS);

        policy.onInsert(a);
        policy.onInsert(b);
        policy.onInsert(c);
        assertEquals(List.of(a, b, c), drain(policy), "读入序即 old 子链淘汰序");

        clock.set(500); // < oldBlocksTime
        policy.onAccess(b);

        // plain LRU 会得到 [a, c, b]；midpoint LRU 窗内不动 → 仍 [a, b, c]
        assertEquals(List.of(a, b, c), drain(policy), "窗内重访不提升、不重排");
    }

    /**
     * 过窗重访提升到 new：old 页驻留超过 {@code oldBlocksTime} 后被访问 → 升 new 子链。
     * 用"随后读入一张新页 d"作判别器：若 b 真在 new，则 fresh 的 d（落 old）淘汰序在 b 之前；
     * 若 b 只是被移到单链 MRU（plain LRU），则 b 会排在 d 之前。断言 [a, c, d, b] 证明 b 进了 new。
     */
    @Test
    void pointLookupPromotesToNewAfterOldWindow() {
        MidpointLruReplacementPolicy policy = newPolicy();
        BufferFrame a = new BufferFrame(PS);
        BufferFrame b = new BufferFrame(PS);
        BufferFrame c = new BufferFrame(PS);
        BufferFrame d = new BufferFrame(PS);

        policy.onInsert(a);
        policy.onInsert(b);
        policy.onInsert(c);

        clock.set(OLD_BLOCKS_TIME_MS + 500); // 超过提升窗
        policy.onAccess(b);                  // b 升 new

        policy.onInsert(d);                  // d 落 old midpoint

        assertEquals(List.of(a, c, d, b), drain(policy),
                "b 升 new 后比 fresh 的 d 更难淘汰");
    }

    /**
     * 建一条已提升的 new 子链 [n0..n7]（front 最易淘汰、back=young 头）：先全部读入 old，跨过提升窗后逐个访问升 new。
     */
    private List<BufferFrame> buildPromotedNewList(MidpointLruReplacementPolicy policy, int count) {
        List<BufferFrame> frames = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            BufferFrame f = new BufferFrame(PS);
            frames.add(f);
            policy.onInsert(f); // clock=0 进 old
        }
        clock.set(OLD_BLOCKS_TIME_MS); // 跨过提升窗
        for (BufferFrame f : frames) {
            policy.onAccess(f); // 逐个升 new，按序进 young 头
        }
        return frames;
    }

    /** 命中 new 子链且离 young 头较远的页，再访问时提升到 young 头（保持热页热）。 */
    @Test
    void reAccessFarFromNewHeadMovesToYoungHead() {
        MidpointLruReplacementPolicy policy = newPolicy();
        List<BufferFrame> n = buildPromotedNewList(policy, 8); // new=[n0..n7]
        assertEquals(n, drain(policy));

        policy.onAccess(n.get(1)); // n1 离 young 头很远 → 移到 young 头

        assertEquals(List.of(n.get(0), n.get(2), n.get(3), n.get(4),
                n.get(5), n.get(6), n.get(7), n.get(1)), drain(policy));
    }

    /** 命中 new 子链且离 young 头很近的页，再访问不改链（youngDistanceThreshold 抗抖动），避免热点页频繁改链。 */
    @Test
    void reAccessNearNewHeadDoesNotChurn() {
        MidpointLruReplacementPolicy policy = newPolicy();
        List<BufferFrame> n = buildPromotedNewList(policy, 8); // new=[n0..n7]，n7 为 young 头

        policy.onAccess(n.get(6)); // n6 紧邻 young 头（距头 1）→ 不应移动

        assertEquals(n, drain(policy), "近 young 头重访不改链");
    }

    private static List<BufferFrame> drain(MidpointLruReplacementPolicy policy) {
        List<BufferFrame> out = new ArrayList<>();
        for (BufferFrame f : policy.victimOrder()) {
            out.add(f);
        }
        return out;
    }
}
