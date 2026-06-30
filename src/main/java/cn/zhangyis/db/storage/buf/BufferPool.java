package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

import java.util.List;
import java.util.Optional;
import java.time.Duration;
import cn.zhangyis.db.domain.SpaceId;

/**
 * Buffer Pool 门面：在 fil.io.PageStore 之上提供受控页访问（fix + S/X page latch + LRU 淘汰 + 脏页写回）。
 * 消费方（未来 fsp）经它拿受控页，不直接接触 PageStore 或文件。
 *
 * <p>简化点：不带 MTR；flush 不做 WAL 门控 / doublewrite；miss/evict/flush 的盘 IO 在内部 poolLock 串行。
 */
public interface BufferPool extends AutoCloseable {

    /**
     * 取得页（命中或读穿），固定并按 mode 取 page latch，返回 RAII 句柄。
     *
     * @param pageId 目标页。
     * @param mode S 或 X。
     * @return 受控页句柄；用完 close。
     */
    PageGuard getPage(PageId pageId, PageLatchMode mode);

    /**
     * 页创建：为页建立"不读盘"的零帧（页须已被 PageStore.extend 在盘上分配/零填充，由调用方保证）。
     * **要求 X latch**（创建/重初始化是写操作）；若该页已驻留，则**重初始化**（在取得 X latch 后清零、复用帧，
     * 对齐 InnoDB buf_page_create）——调用方须确保该页确实在被（重新）分配，不能误覆盖在用页。
     *
     * @param pageId 新页（驻留则被重初始化清零）。
     * @param mode   必须为 EXCLUSIVE，否则抛 DatabaseValidationException。
     * @return 受控页句柄（X latch）。
     */
    PageGuard newPage(PageId pageId, PageLatchMode mode);

    /**
     * Read-ahead 预取（§8.1）：若该页未驻留且有空闲帧，则异步语义地把页载入到 LRU old 子链——**不 fix、不记访问
     * （不提升）**，使未被真实访问的预取页留在 old 子链、最先被淘汰。已驻留/正在载入则跳过；无空闲帧直接丢弃
     * （read-ahead 不淘汰脏页、不挤占前台需求读）；载入失败尽力而为丢弃，不留 LOADING 占位。
     *
     * @param pageId 预取目标页。
     */
    void prefetch(PageId pageId);

    /** 若该页驻留、未 fix 且为脏，则写回 PageStore 并清脏。 */
    void flush(PageId pageId);

    /** 写回所有未 fix 的脏页。 */
    void flushAll();

    /**
     * 返回 flush list 候选快照，按 oldestModificationLsn 升序排列，只包含 oldest <= targetLsn 的脏页。
     * Flush 模块不能通过该视图持有 frame 或 latch；真正写盘前必须再调用 {@link #snapshotForFlush(PageId)} 重新确认。
     *
     * @param targetLsn flush 目标 LSN。
     * @param maxPages 最多返回页数，0 表示只探测不返回。
     * @return 不可变候选列表。
     */
    List<DirtyPageCandidate> dirtyPageCandidates(Lsn targetLsn, int maxPages);

    /**
     * 尝试复制一个未 fixed 的脏页镜像。返回 empty 表示页不存在、已 clean、仍被前台 guard fixed 或不适合本轮 flush。
     *
     * @param pageId 目标页。
     * @return 可跨 IO 使用的稳定页副本。
     */
    Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId);

    /**
     * flush 成功后回调 Buffer Pool。只有当 frame 仍是同一页、仍 dirty、未 fixed、dirtyVersion/pageLSN 与 snapshot
     * 一致时才清脏；若 snapshot 后页面再次变脏，则保持 dirty，避免把新修改误标 clean。
     *
     * @param snapshot 已写盘的页副本。
     * @return true 表示页被标 clean；false 表示仍保留 dirty。
     */
    boolean completeFlush(FlushPageSnapshot snapshot);

    /**
     * flush 失败回调。F1 dirty 状态没有 FLUSHING 中间态，因此当前实现只校验页号并保留 dirty，供后续重试。
     *
     * @param pageId 失败页。
     */
    void failFlush(PageId pageId);

    /**
     * 返回当前最老 dirty LSN；如果没有脏页，返回调用方传入的 cleanBoundary。
     *
     * @param cleanBoundary 无脏页时的安全边界。
     * @return oldest dirty LSN 或 cleanBoundary。
     */
    Lsn oldestDirtyLsnOr(Lsn cleanBoundary);

    /**
     * 当前是否存在任何 dirty frame。Checkpoint 需要区分“没有脏页，可推进到 redo durable 边界”和
     * “存在脏页，必须同时受 oldest dirty 与 redo closed LSN 限制”这两种语义。
     *
     * @return 任意驻留帧仍为 dirty 时返回 true。
     */
    boolean hasDirtyPages();

    /**
     * 截断前排空目标表空间：在限定时间内等待全部 fix 归零，确认不存在脏帧后移除所有驻留帧。
     * 该方法不会隐式 flush；发现 dirty 必须抛错，避免绕过 WAL/doublewrite/checkpoint 顺序静默丢页。
     * 调用方必须已持该表空间独占 operation lease，阻止新的 page fix 与并发 flush 进入。
     *
     * @param spaceId 目标表空间。
     * @param timeout 等待 fix 归零的正超时。
     */
    void invalidateTablespace(SpaceId spaceId, Duration timeout);

    /**
     * 统计某连续页区间 {@code [firstPageNo, firstPageNo+pageCount)} 内当前已驻留的页数（含正在载入的占位页）。
     * 供 random read-ahead（§8.3）判定「同一 extent 已有足够多页驻留」：调用方传入页所在 extent 的起始页 + extent
     * 页数，命中阈值则补取整 extent。实现在 poolLock 内对区间逐页查 {@code residentMap}（O(pageCount) 次查找，
     * 与池大小无关），故 random 启用时每次访问引入一次 O(extent) 开销；random 默认禁用时调用方不会调用本方法。
     *
     * @param spaceId     目标表空间。
     * @param firstPageNo 区间起始页号（含，≥0）。
     * @param pageCount   区间页数（≥1）。
     * @return 区间内已驻留页数（0..pageCount）。
     */
    int residentCountInRange(SpaceId spaceId, long firstPageNo, int pageCount);

    /** 帧总容量。 */
    int capacity();

    /** 当前驻留帧数。 */
    int residentCount();

    /**
     * 快照当前驻留页的 {@code PageId} 列表（只读定位信息，不含页体）。供 warmup dump 在 close 时保存热页定位。
     * 实现在内部短临界区内拷贝，调用方拿到的是不可变快照。
     *
     * @return 驻留页 PageId 不可变快照。
     */
    List<PageId> residentPageIds();

    /** 关闭：flushAll 后释放（假设无活跃句柄）。 */
    @Override
    void close();
}
