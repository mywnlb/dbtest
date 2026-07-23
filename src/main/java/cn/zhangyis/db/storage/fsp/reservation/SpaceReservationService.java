package cn.zhangyis.db.storage.fsp.reservation;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.exception.SpaceReservationExceededException;
import cn.zhangyis.db.storage.fsp.extent.ExtentManagementRegionLayout;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 表空间预留服务（disk-manager §7.1）。它在多页操作真正分配 page 前完成容量预检和必要扩容，并用内存态账本
 * 防止并发操作把同一批未来 extent 重复承诺给多个 MTR。
 *
 * <p>首版语义是“容量 guard”而非持久资源分配：reservation 不把具体页号或 extent 绑定给调用方，只保证在创建
 * reservation 的时刻，page0 currentSize 和物理文件已经足以材料化所需完整 extent。真正的页/extent 归属仍由
 * {@code SegmentPageAllocator} 按 FSP 元数据决定。
 *
 * <p>并发边界：{@link #lock} 只保护按表空间聚合的容量承诺计数。它不得包住任何可能进入 Buffer Pool 或等待
 * page latch 的调用；否则 B+Tree split 线程可能一边持 index page latch 等 reservation lock，另一边持
 * reservation lock 等 page0 latch，形成物理 latch 死锁。活动 reservation 索引用并发集合维护，allocatePage
 * 消费页额度只修改当前 MTR 的 reservation 原子字段，不等待全局账本锁。
 */
public final class SpaceReservationService {

    /** 物理文件门面；reserve 阶段用 ensureCapacity 先把文件扩到需要的页数。 */
    private final PageStore pageStore;

    /** 实例页大小；用于把 page quota 换算成完整 extent quota。 */
    private final PageSize pageSize;

    /** page0 仓储；读取 freeLimit/currentSize，并在扩容成功后推进 currentSize。 */
    private final SpaceHeaderRepository headerRepo;

    /** FLST 原语；用于读取全局 FSP_FREE 链长度，估算可承诺的完整 extent。 */
    private final Flst flst;

    /** 识别重复管理区首 extent，防止 reservation 把固定 XDES/bitmap 容量承诺给业务页。 */
    private final ExtentManagementRegionLayout managementLayout;

    /** 容量承诺账本锁。不得在持有该锁时进入 Buffer Pool、page latch、事务锁等待或上层 SQL/session 回调。 */
    private final ReentrantLock lock = new ReentrantLock();

    /** 按表空间聚合的容量账本。 */
    private final Map<SpaceId, ReservationCounter> countersBySpace = new HashMap<>();

    /** 按 MTR id 索引的活动 reservation；consume 路径无全局锁读取，避免持 data page latch 时阻塞在账本锁。 */
    private final Map<Long, CopyOnWriteArrayList<SpaceReservation>> reservationsByMtr = new ConcurrentHashMap<>();

    /**
     * 创建 {@code SpaceReservationService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param headerRepo 由组合根提供的 {@code SpaceHeaderRepository} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param flst FSP 层的链表、空间预留或分配方向对象；不得为 {@code null}，必须属于当前表空间且保持 extent/segment 所有权不变量
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SpaceReservationService(PageStore pageStore, PageSize pageSize,
                                   SpaceHeaderRepository headerRepo, Flst flst) {
        if (pageStore == null || pageSize == null || headerRepo == null || flst == null) {
            throw new DatabaseValidationException("space reservation dependencies must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.flst = flst;
        this.managementLayout = new ExtentManagementRegionLayout(pageSize);
    }

    /**
     * 创建一次表空间预留。数据流为：校验调用方预算 → 在不持账本锁的情况下读取 page0/FLST 容量快照 → 读取
     * 内存承诺计数 → 如不足先扩物理文件并推进 page0 currentSize → 最后用账本锁发布 reservation。
     *
     * <p>这里刻意不用账本锁包住 page0/FLST 访问。B+Tree split 可能在持 index page latch 时消费 reservation；
     * 若并发 reserve 线程持账本锁再等待 page0，而 split 线程持 page latch 再等待账本锁，会形成物理 latch 环。
     * 同一表空间的 reserve 之间由 page0 X latch 串行；release 只会降低已承诺容量，因此用稍旧的 committed 计数最多
     * 导致保守扩容，不会承诺不足。
     *
     * @param mtr 当前活动 MTR；reservation 会按 mtr id 绑定，供后续 allocatePage 消费。
     * @param spaceId 目标表空间。
     * @param kind 预留类型。
     * @param pages 本次操作最多会创建的数据页数。
     * @param extents 本次操作额外需要保底的完整 extent 数。
     * @return 活动 reservation，由调用方或 MTR memo 关闭释放。
     */
    public SpaceReservation reserve(MiniTransaction mtr, SpaceId spaceId, SpaceReservationKind kind,
                                    long pages, long extents) {
        requireReservationArgs(mtr, spaceId, kind, pages, extents);
        long capacityExtents = capacityExtents(pages, extents);
        SpaceHeaderSnapshot header = headerRepo.readForUpdate(mtr, spaceId);
        long freeExtents = flst.length(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId));
        long reservedCapacityExtents = reservedCapacityExtents(spaceId);
        long currentSize = header.currentSizeInPages().value();
        long targetSize = targetSizeForCapacity(spaceId, header, capacityExtents,
                freeExtents, reservedCapacityExtents);
        if (targetSize > currentSize) {
            PageNo target = PageNo.of(targetSize);
            pageStore.ensureCapacity(spaceId, target);
            headerRepo.setCurrentSizeInPages(mtr, spaceId, target);
        }
        SpaceReservation reservation = new SpaceReservation(this, mtr.id(), spaceId, kind,
                pages, extents, capacityExtents);
        lock.lock();
        try {
            countersBySpace.computeIfAbsent(spaceId, ignored -> new ReservationCounter())
                    .reserve(capacityExtents);
            reservationsByMtr.computeIfAbsent(mtr.id(), ignored -> new CopyOnWriteArrayList<>()).add(reservation);
            return reservation;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 如果当前 MTR 在该表空间存在活动 reservation，则消费一个 page quota；若 quota 已耗尽，在真正进入
     * SegmentPageAllocator 前抛出领域异常。没有活动 reservation 的兼容路径直接放行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 待分配页所属表空间。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SpaceReservationExceededException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    public void consumePageIfReserved(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || spaceId == null) {
            throw new DatabaseValidationException("reservation consume mtr/space id must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        List<SpaceReservation> reservations = reservationsByMtr.get(mtr.id());
        if (reservations == null) {
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        boolean exhaustedForSpace = false;
        for (SpaceReservation reservation : reservations) {
            SpaceReservation.ConsumeResult result = reservation.consumePageQuota(spaceId);
            if (result == SpaceReservation.ConsumeResult.CONSUMED) {
                return;
            }
            if (result == SpaceReservation.ConsumeResult.EXHAUSTED) {
                exhaustedForSpace = true;
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        if (exhaustedForSpace) {
            throw new SpaceReservationExceededException("space reservation page quota exhausted: mtr="
                    + mtr.id() + " space=" + spaceId.value());
        }
    }

    /**
     * 释放 reservation 的剩余容量承诺。该方法只由 {@link SpaceReservation#close()} 调用，并在锁内做幂等保护。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param reservation 待释放 reservation。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void release(SpaceReservation reservation) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (reservation == null) {
            throw new DatabaseValidationException("space reservation must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (!reservation.markClosed()) {
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        lock.lock();
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        try {
            ReservationCounter counter = countersBySpace.get(reservation.spaceId);
            if (counter != null) {
                counter.release(reservation.reservedCapacityExtents);
                if (counter.reservedCapacityExtents == 0L) {
                    countersBySpace.remove(reservation.spaceId);
                }
            }
            CopyOnWriteArrayList<SpaceReservation> reservations = reservationsByMtr.get(reservation.mtrId);
            if (reservations != null) {
                reservations.remove(reservation);
                if (reservations.isEmpty()) {
                    reservationsByMtr.remove(reservation.mtrId);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 计算 {@code reservedCapacityExtents} 所表达的表空间、区与段分配数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code reservedCapacityExtents} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    private long reservedCapacityExtents(SpaceId spaceId) {
        lock.lock();
        try {
            ReservationCounter counter = countersBySpace.get(spaceId);
            return counter == null ? 0L : counter.reservedCapacityExtents;
        } finally {
            lock.unlock();
        }
    }

    private long targetSizeForCapacity(SpaceId spaceId, SpaceHeaderSnapshot header, long neededCapacityExtents,
                                       long freeExtents, long reservedCapacityExtents) {
        long currentSize = header.currentSizeInPages().value();
        long freeLimit = header.freeLimitPageNo().value();
        if (freeLimit > currentSize) {
            throw new FspMetadataException("freeLimit exceeds current size: freeLimit="
                    + freeLimit + " currentSize=" + currentSize + " space=" + spaceId.value());
        }
        long available = freeExtents - reservedCapacityExtents;
        long targetSize = currentSize;
        long probeFreeLimit = freeLimit;
        long pagesPerExtent = pageSize.pagesPerExtent();
        while (available < neededCapacityExtents) {
            long extentNo = Math.floorDiv(probeFreeLimit, pagesPerExtent);
            long nextFreeLimit = multiplyExact(addExact(extentNo, 1L), pagesPerExtent);
            if (nextFreeLimit > targetSize) {
                targetSize = nextFreeLimit;
                continue;
            }
            probeFreeLimit = nextFreeLimit;
            if (!managementLayout.isManagementExtent(extentNo)) {
                available++;
            }
        }
        return targetSize;
    }

    private long capacityExtents(long pages, long extents) {
        long pageExtents = pages == 0L ? 0L : Math.floorDiv(addExact(pages, pageSize.pagesPerExtent() - 1L),
                pageSize.pagesPerExtent());
        return addExact(extents, pageExtents);
    }

    private static void requireReservationArgs(MiniTransaction mtr, SpaceId spaceId, SpaceReservationKind kind,
                                               long pages, long extents) {
        if (mtr == null || spaceId == null || kind == null) {
            throw new DatabaseValidationException("reservation mtr/space/kind must not be null");
        }
        if (pages < 0L || extents < 0L) {
            throw new DatabaseValidationException("reservation pages/extents must be non-negative: pages="
                    + pages + " extents=" + extents);
        }
        if (pages == 0L && extents == 0L) {
            throw new DatabaseValidationException("reservation must request at least one page or extent");
        }
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("space reservation arithmetic overflow", overflow);
        }
    }

    private static long multiplyExact(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("space reservation target size overflow", overflow);
        }
    }

    /** 单表空间容量账本。 */
    private static final class ReservationCounter {

        /** 已承诺但尚未关闭的完整 extent 容量数。 */
        private long reservedCapacityExtents;

        private void reserve(long extents) {
            reservedCapacityExtents = addExact(reservedCapacityExtents, extents);
        }

        private void release(long extents) {
            reservedCapacityExtents -= extents;
            if (reservedCapacityExtents < 0L) {
                throw new FspMetadataException("space reservation counter became negative");
            }
        }
    }
}
