package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Random read-ahead 检测器（§8.3）。与 linear read-ahead 互补：linear 按「同一 extent 内顺序访问」触发，random 按
 * 「同一 extent 内已驻留页数」触发——不要求顺序。当被访问页所在 extent 在 Buffer Pool 内的驻留页数达到 {@code threshold}
 * 时，产出对**整个 extent** 的预取请求（{@code prefetch} 自身会跳过已驻留页，故只补取缺失页）。
 *
 * <p>MySQL 默认 {@code innodb_random_read_ahead=OFF}，是较次要的启发式；本项目同样默认禁用（由
 * {@code ReadAheadService} 以 threshold 0 表达「不构造本检测器」）。
 *
 * <p><b>去重（关键约束）</b>：同一 extent 只在一个 <em>bounded recent</em> 窗口内去重，<b>不做永久 set</b>。
 * 若用永久 set，extent 一旦发过预取就永远不能再次触发——可该 extent 的页随后可能被淘汰，需要再次补取。
 * 采用容量受限的 FIFO 最近窗口：新 extent 发出后入窗，窗满则挤出最旧条目；被挤出的 extent 之后可再次触发。
 *
 * <p>简化点（教学）：触发条件用「同一 extent 驻留页数量」而非 InnoDB 更细的连续/access-bit 启发式；后续可用
 * access-bit / evicted-without-access 统计细化。extent 大小用本地常量 {@link #PAGES_PER_EXTENT}，不依赖 {@code fsp}
 * （守「buf 不解析空间结构」）。
 *
 * <p><b>线程模型</b>：本类不自带锁，由调用方（{@code ReadAheadService}）在自己的临界区内串行调用 {@link #record}。
 */
public final class RandomReadAheadDetector {

    /** read-ahead extent 粒度（与 InnoDB extent 对齐为 64 页）；复用 linear 常量，避免依赖 fsp。 */
    public static final int PAGES_PER_EXTENT = LinearReadAheadTracker.PAGES_PER_EXTENT;

    /** 默认 recent 去重窗口容量；足够覆盖近期热点 extent，又远小于全空间避免退化为永久 set。 */
    private static final int DEFAULT_RECENT_WINDOW = 64;

    /** 触发预取的同一 extent 驻留页数阈值（1..64）。驻留页数 ≥ 该值且未刚发过则产出整 extent 请求。 */
    private final int threshold;
    /** recent 去重窗口容量（≥1）；窗满挤出最旧条目，使被挤出的 extent 之后可再次触发。 */
    private final int recentWindow;

    /** FIFO 最近发出预取的 extent，用于挤出最旧条目；与 {@link #recentSet} 同步维护。 */
    private final Deque<ExtentKey> recentOrder = new ArrayDeque<>();
    /** recent 窗口成员集合，O(1) 判定某 extent 是否近期已发过（去重）。 */
    private final Set<ExtentKey> recentSet = new HashSet<>();

    /** 默认窗口构造。 */
    public RandomReadAheadDetector(int threshold) {
        this(threshold, DEFAULT_RECENT_WINDOW);
    }

    /**
     * @param threshold    同一 extent 驻留页数触发阈值（1..64）。
     * @param recentWindow recent 去重窗口容量（≥1）；测试可注入小窗口验证窗口前移后可再次触发。
     */
    public RandomReadAheadDetector(int threshold, int recentWindow) {
        if (threshold < 1 || threshold > PAGES_PER_EXTENT) {
            throw new DatabaseValidationException("random read-ahead threshold must be in [1, " + PAGES_PER_EXTENT
                    + "]: " + threshold);
        }
        if (recentWindow < 1) {
            throw new DatabaseValidationException("random read-ahead recent window must be >= 1: " + recentWindow);
        }
        this.threshold = threshold;
        this.recentWindow = recentWindow;
    }

    /**
     * 记录一次页访问及其所在 extent 的驻留页数，必要时产出整 extent 预取请求。
     *
     * <p>数据流：调用方（service）已在 poolLock 内查得被访问页所在 extent 的驻留页数 {@code residentInExtent}，
     * 连同 pageId 传入。本方法判定驻留数是否达阈值→若达阈值且该 extent 不在 recent 去重窗内，则把它登记入窗
     * （窗满挤出最旧）并产出对整 extent 的请求；否则返回 empty。
     *
     * @param pageId           被访问的页（其 extent 即预取目标）。
     * @param residentInExtent 该页所在 extent 当前在 Buffer Pool 内的驻留页数（0..64），由调用方查得。
     * @return 若应预取整 extent，则为对应请求；否则 empty。
     */
    public Optional<ReadAheadRequest> record(PageId pageId, int residentInExtent) {
        if (pageId == null) {
            throw new DatabaseValidationException("random read-ahead access pageId must not be null");
        }
        if (residentInExtent < 0) {
            throw new DatabaseValidationException("random read-ahead resident count must be >= 0: " + residentInExtent);
        }
        if (residentInExtent < threshold) {
            return Optional.empty();
        }
        SpaceId space = pageId.spaceId();
        long extent = pageId.pageNo().value() / PAGES_PER_EXTENT;
        ExtentKey key = new ExtentKey(space.value(), extent);
        if (recentSet.contains(key)) {
            return Optional.empty(); // 仍在 bounded recent 窗内：去重，不重复提交。
        }
        rememberEmitted(key);
        return Optional.of(new ReadAheadRequest(space, extent * PAGES_PER_EXTENT, PAGES_PER_EXTENT));
    }

    /** 登记一个刚发出的 extent 到 recent 窗；窗满则挤出最旧条目（FIFO），使其之后可再次触发。 */
    private void rememberEmitted(ExtentKey key) {
        recentOrder.addLast(key);
        recentSet.add(key);
        if (recentOrder.size() > recentWindow) {
            ExtentKey evicted = recentOrder.pollFirst();
            recentSet.remove(evicted);
        }
    }

    /** recent 去重窗口的键：(spaceId, extentNo)。record 默认 equals/hashCode 提供值语义，供 set/deque 成员判定。 */
    private record ExtentKey(int spaceId, long extent) {
    }
}
