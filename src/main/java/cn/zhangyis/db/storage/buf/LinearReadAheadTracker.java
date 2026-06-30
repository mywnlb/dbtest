package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;

import java.util.Optional;

/**
 * Linear read-ahead 检测器（§8.2）。跟踪「单顺序流」：同一表空间内连续升序访问；当当前 extent 内连续访问页数达到
 * {@code threshold} 时，产出对**下一个 extent** 的预取请求。
 *
 * <p>简化点（教学）：只跟踪一条顺序流（多流交错访问会重置当前 run），不像 InnoDB 用 page hash 内的 access bit 做
 * per-extent 统计；extent 大小用本地常量 {@link #PAGES_PER_EXTENT}，不依赖 {@code fsp}（守「buf 不解析空间结构」）。
 *
 * <p><b>线程模型</b>：本类不自带锁，由调用方（{@code ReadAheadService}）在自己的临界区内串行调用 {@link #record}。
 */
public final class LinearReadAheadTracker {

    /** read-ahead extent 粒度（与 InnoDB extent 对齐为 64 页）；本地常量，避免依赖 fsp。 */
    public static final int PAGES_PER_EXTENT = 64;

    /** 同一 extent 内触发预取的连续访问阈值（InnoDB 默认 56，范围 1..64）。 */
    private final int threshold;

    /** 当前顺序流所属表空间。 */
    private SpaceId runSpace;
    /** 是否已有上一次访问记录。 */
    private boolean hasLast;
    /** 上一次访问页号（用于判定 +1 连续）。 */
    private long lastPageNo;
    /** 当前 run 所在 extent。 */
    private long runExtent;
    /** 当前 run 已连续访问页数。 */
    private int runLength;
    /** 去重：最近一次已发出预取的 (space, nextExtent)，避免同一 extent 重复提交。 */
    private SpaceId emittedSpace;
    private long emittedNextExtent = -1;

    public LinearReadAheadTracker(int threshold) {
        if (threshold < 1 || threshold > PAGES_PER_EXTENT) {
            throw new DatabaseValidationException("read-ahead threshold must be in [1, " + PAGES_PER_EXTENT
                    + "]: " + threshold);
        }
        this.threshold = threshold;
    }

    /**
     * 记录一次页访问，必要时产出下一个 extent 的预取请求。
     *
     * <p>数据流：判定本次访问相对上次是否「同表空间、页号 +1」的顺序访问→在同一 extent 内则累计 run、顺序跨入下一
     * extent 则重置为新 extent 的 run、乱序/反向/换表空间则重置 run；run 达阈值且该「下一 extent」未发过则产出请求。
     *
     * @param pageId 被访问的页。
     * @return 若应预取下一 extent，则为对应请求；否则 empty。
     */
    public Optional<ReadAheadRequest> record(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("read-ahead access pageId must not be null");
        }
        SpaceId space = pageId.spaceId();
        long pageNo = pageId.pageNo().value();
        long extent = pageNo / PAGES_PER_EXTENT;

        boolean sequential = hasLast && space.equals(runSpace) && pageNo == lastPageNo + 1;
        if (sequential && extent == runExtent) {
            runLength++;
        } else if (sequential && extent == runExtent + 1) {
            // 顺序跨入下一 extent：新 extent 重新计 run（per-extent 阈值语义）。
            runExtent = extent;
            runLength = 1;
        } else {
            // 乱序 / 反向 / 换表空间 / 首次：重置为新 run。
            runSpace = space;
            runExtent = extent;
            runLength = 1;
        }
        lastPageNo = pageNo;
        hasLast = true;

        if (runLength >= threshold) {
            long nextExtent = runExtent + 1;
            boolean alreadyEmitted = space.equals(emittedSpace) && nextExtent == emittedNextExtent;
            if (!alreadyEmitted) {
                emittedSpace = space;
                emittedNextExtent = nextExtent;
                return Optional.of(new ReadAheadRequest(space, nextExtent * PAGES_PER_EXTENT, PAGES_PER_EXTENT));
            }
        }
        return Optional.empty();
    }
}
