package cn.zhangyis.db.storage.fsp.reservation;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.exception.SpaceReservationExceededException;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 表空间预留服务（disk-manager §7.1）。它在多页操作真正分配 page 前完成容量预检和必要扩容，并用内存态账本
 * 防止并发操作把同一批未来 extent 重复承诺给多个 MTR。
 *
 * <p>首版语义是“容量 guard”而非持久资源分配：reservation 不把具体页号或 extent 绑定给调用方，只保证在创建
 * reservation 的时刻，page0 currentSize 和物理文件已经足以材料化所需完整 extent。真正的页/extent 归属仍由
 * {@code SegmentPageAllocator} 按 FSP 元数据决定。
 *
 * <p>并发边界：{@link #lock} 保护全部 reservation 账本；它只在 reserve/consume/release 的短路径内持有。
 * reserve 会在该锁内读取并可能更新 page0 currentSize，从而保证“检查容量 → 发布 reservation”不可被其它
 * reservation 穿插。该锁不回调 Buffer Pool 以外的上层模块，也不参与事务锁等待。
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

    /** reservation 账本锁。不得在持有该锁时进入事务锁等待或执行上层 SQL/session 回调。 */
    private final ReentrantLock lock = new ReentrantLock();

    /** 按表空间聚合的容量账本。 */
    private final Map<SpaceId, ReservationCounter> countersBySpace = new HashMap<>();

    /** 按 MTR id 索引的活动 reservation，allocatePage 消费时据此找到当前操作的页额度。 */
    private final Map<Long, List<SpaceReservation>> reservationsByMtr = new HashMap<>();

    public SpaceReservationService(PageStore pageStore, PageSize pageSize,
                                   SpaceHeaderRepository headerRepo, Flst flst) {
        if (pageStore == null || pageSize == null || headerRepo == null || flst == null) {
            throw new DatabaseValidationException("space reservation dependencies must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.flst = flst;
    }

    /**
     * 创建一次表空间预留。数据流为：校验调用方预算 → 读取 page0 容量快照 → 计算已可材料化 extent 与已承诺 extent
     * 的差额 → 如不足先扩物理文件并推进 page0 currentSize → 把 reservation 登记到内存账本。
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
        lock.lock();
        try {
            SpaceHeaderSnapshot header = headerRepo.readForUpdate(mtr, spaceId);
            long currentSize = header.currentSizeInPages().value();
            long targetSize = targetSizeForCapacity(mtr, spaceId, header, capacityExtents);
            if (targetSize > currentSize) {
                PageNo target = PageNo.of(targetSize);
                pageStore.ensureCapacity(spaceId, target);
                headerRepo.setCurrentSizeInPages(mtr, spaceId, target);
            }
            SpaceReservation reservation = new SpaceReservation(this, mtr.id(), spaceId, kind,
                    pages, extents, capacityExtents);
            countersBySpace.computeIfAbsent(spaceId, ignored -> new ReservationCounter())
                    .reserve(capacityExtents);
            reservationsByMtr.computeIfAbsent(mtr.id(), ignored -> new ArrayList<>()).add(reservation);
            return reservation;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 如果当前 MTR 在该表空间存在活动 reservation，则消费一个 page quota；若 quota 已耗尽，在真正进入
     * SegmentPageAllocator 前抛出领域异常。没有活动 reservation 的兼容路径直接放行。
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 待分配页所属表空间。
     */
    public void consumePageIfReserved(MiniTransaction mtr, SpaceId spaceId) {
        if (mtr == null || spaceId == null) {
            throw new DatabaseValidationException("reservation consume mtr/space id must not be null");
        }
        lock.lock();
        try {
            List<SpaceReservation> reservations = reservationsByMtr.get(mtr.id());
            if (reservations == null) {
                return;
            }
            boolean activeForSpace = false;
            for (SpaceReservation reservation : reservations) {
                if (!reservation.closed && reservation.spaceId.equals(spaceId)) {
                    activeForSpace = true;
                    if (reservation.remainingPages > 0) {
                        reservation.remainingPages--;
                        return;
                    }
                }
            }
            if (activeForSpace) {
                throw new SpaceReservationExceededException("space reservation page quota exhausted: mtr="
                        + mtr.id() + " space=" + spaceId.value());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放 reservation 的剩余容量承诺。该方法只由 {@link SpaceReservation#close()} 调用，并在锁内做幂等保护。
     *
     * @param reservation 待释放 reservation。
     */
    void release(SpaceReservation reservation) {
        if (reservation == null) {
            throw new DatabaseValidationException("space reservation must not be null");
        }
        lock.lock();
        try {
            if (reservation.closed) {
                return;
            }
            reservation.closed = true;
            ReservationCounter counter = countersBySpace.get(reservation.spaceId);
            if (counter != null) {
                counter.release(reservation.reservedCapacityExtents);
                if (counter.reservedCapacityExtents == 0L) {
                    countersBySpace.remove(reservation.spaceId);
                }
            }
            List<SpaceReservation> reservations = reservationsByMtr.get(reservation.mtrId);
            if (reservations != null) {
                for (Iterator<SpaceReservation> it = reservations.iterator(); it.hasNext(); ) {
                    if (it.next() == reservation) {
                        it.remove();
                        break;
                    }
                }
                if (reservations.isEmpty()) {
                    reservationsByMtr.remove(reservation.mtrId);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private long targetSizeForCapacity(MiniTransaction mtr, SpaceId spaceId,
                                       SpaceHeaderSnapshot header, long neededCapacityExtents) {
        long currentSize = header.currentSizeInPages().value();
        long freeLimit = header.freeLimitPageNo().value();
        if (freeLimit > currentSize) {
            throw new FspMetadataException("freeLimit exceeds current size: freeLimit="
                    + freeLimit + " currentSize=" + currentSize + " space=" + spaceId.value());
        }
        long freeExtents = flst.length(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId));
        ReservationCounter counter = countersBySpace.get(spaceId);
        long reserved = counter == null ? 0L : counter.reservedCapacityExtents;
        long available = freeExtents - reserved;
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
            if (extentNo != 0L) {
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
