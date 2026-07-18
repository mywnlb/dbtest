package cn.zhangyis.db.storage.buf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Midpoint LRU 替换策略（设计 §6.1 / §6.4）。把 LRU 拆成 new（热）/ old（冷+新读入）两条子链，
 * 抵抗一次性大扫描冲掉 OLTP 工作集：
 *
 * <ul>
 *   <li>读入页（{@link #onInsert}）一律落 old 子链头部（midpoint），不直接进 new 头。</li>
 *   <li>命中 old 页只有在其驻留 old 子链超过 {@code oldBlocksTime} 后再次访问才提升到 new 头；
 *       窗口内的重复访问（典型为扫描）不提升，从而不污染 new 子链。</li>
 *   <li>命中 new 页若离 new 头过近（{@code youngDistanceThreshold} 内）则不移动，避免热点页频繁改链。</li>
 * </ul>
 *
 * <p><b>淘汰序约定</b>：{@link #victimOrder()} 迭代序 = 淘汰优先序（最易淘汰在前）= old 子链整体先于 new 子链，
 * 每条子链内部 front 最易淘汰、back 最近访问（least evictable）。
 *
 * <p><b>并发归属</b>：所有方法由 BufferPool 在 list/meta 兼容锁下调用，实现自身无需线程安全（与 {@link ReplacementPolicy} 约定一致）。
 *
 * <p><b>简化点</b>：① 提升窗用注入的毫秒时钟近似 InnoDB 的 freed_page_clock（访问计数），墙钟回拨只影响提升时机、
 * 不破坏正确性；② 暂不强制 old/new 的 {@code oldBlocksPct} 容量配比再平衡（new 不会无界膨胀至挤空 old 由上层容量与
 * 工作集规模约束），完整配比再平衡留后续；③ {@code youngDistanceThreshold} 取 new 子链长度的相对比例。
 */
final class MidpointLruReplacementPolicy implements ReplacementPolicy {

    /** 提升窗（毫秒，设计默认 1000ms）：old 页驻留满该时长后再次访问才升 new，窗内重访不升以抗扫描污染。 */
    private static final long OLD_BLOCKS_TIME_MILLIS = 1000L;

    /** youngDistanceThreshold 取 young 子链长度的 1/N（此处 N=4，近似 InnoDB 的 1/4 规则）：距 young 头此阈值内不改链。 */
    private static final int YOUNG_DISTANCE_DIVISOR = 4;

    /** 注入的毫秒时钟；用于判断 old 页是否已驻留超过 {@code oldBlocksTime}。生产为 {@code System::currentTimeMillis}。 */
    private final LongSupplier clockMillis;

    /** old 子链：冷页与新读入页。迭代序 front=最易淘汰（真 LRU 尾），back=midpoint（最近读入，least evictable）。 */
    private final LinkedHashSet<BufferFrame> oldList = new LinkedHashSet<>();

    /** new 子链：热页。迭代序 front=最易淘汰，back=young 头（MRU）。整体比 old 子链更难淘汰。 */
    private final LinkedHashSet<BufferFrame> newList = new LinkedHashSet<>();

    /** 各 old 页进入 old 子链的毫秒时刻；只对 old 子链成员有效，提升或移除时清除。用于 {@code oldBlocksTime} 窗判定。 */
    private final Map<BufferFrame, Long> oldSinceMillis = new HashMap<>();

    /**
     * 创建 {@code MidpointLruReplacementPolicy}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param clockMillis 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     */
    MidpointLruReplacementPolicy(LongSupplier clockMillis) {
        this.clockMillis = clockMillis;
    }

    /**
     * 接收 {@code onAccess} 对应的Buffer Pool生命周期事件；只更新本策略拥有的统计或顺序状态，不接管事件来源资源。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    @Override
    public void onAccess(BufferFrame frame) {
        Long oldSince = oldSinceMillis.get(frame);
        if (oldSince != null) {
            // 命中 old 页：仅当驻留满提升窗才升 new 头；窗内重访（典型为大扫描）不升、不重排 → 抗污染。
            if (clockMillis.getAsLong() - oldSince >= OLD_BLOCKS_TIME_MILLIS) {
                oldList.remove(frame);
                oldSinceMillis.remove(frame);
                newList.add(frame); // 入 new 子链 young 头（back，least evictable）
            }
            return;
        }
        // 命中 new 页：仅当离 young 头足够远才提升到 young 头；近头页不动，避免热点页频繁改链（抗抖动）。
        if (newList.contains(frame)) {
            int threshold = Math.max(1, newList.size() / YOUNG_DISTANCE_DIVISOR);
            if (distanceFromYoungHead(frame) > threshold) {
                newList.remove(frame);
                newList.add(frame);
            }
        }
    }

    /**
     * 帧距 young 头（new 子链 back / MRU）的位置：0 表示就在 young 头。用于 {@code youngDistanceThreshold} 判定，
     * 近头（距离 ≤ 阈值）的页再访问不改链。线性扫描 new 子链，规模为缓冲池容量级，教学实现可接受。
     */
    private int distanceFromYoungHead(BufferFrame frame) {
        int index = 0;
        int found = -1;
        for (BufferFrame f : newList) {
            if (f == frame) {
                found = index;
                break;
            }
            index++;
        }
        return newList.size() - 1 - found;
    }

    /**
     * 接收 {@code onInsert} 对应的Buffer Pool生命周期事件；只更新本策略拥有的统计或顺序状态，不接管事件来源资源。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    @Override
    public void onInsert(BufferFrame frame) {
        // 读入页进 old 子链 midpoint（= old 子链 back，least evictable of old），记录进入时刻供提升窗判定。
        oldList.add(frame);
        oldSinceMillis.put(frame, clockMillis.getAsLong());
    }

    /**
     * 接收 {@code onRemove} 对应的Buffer Pool生命周期事件；只更新本策略拥有的统计或顺序状态，不接管事件来源资源。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    @Override
    public void onRemove(BufferFrame frame) {
        if (oldList.remove(frame)) {
            oldSinceMillis.remove(frame);
            return;
        }
        newList.remove(frame);
    }

    /**
     * 生成淘汰候选顺序快照：先遍历 old 区，再遍历 new 区；快照与后续 LRU 结构修改隔离，不转移 frame 所有权。
     *
     * @return {@code victimOrder} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    @Override
    public Iterable<BufferFrame> victimOrder() {
        // 快照拼接 old→new；调用方在 list/meta 兼容锁下遍历、逐帧加 frameMutex 校验，快照避免迭代期结构改动风险。
        List<BufferFrame> order = new ArrayList<>(oldList.size() + newList.size());
        order.addAll(oldList);
        order.addAll(newList);
        return order;
    }
}
