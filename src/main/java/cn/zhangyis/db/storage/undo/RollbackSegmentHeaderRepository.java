package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

/**
 * undo 表空间 page3 上 rollback segment header 的仓储（设计 §6.3）。经 MTR 持页 latch 读写 active slot 目录
 * 与 INSERT/UPDATE cached segment 栈，所有写都进 MTR redo（page3 物理 crash-safe）。形态仿
 * {@code SpaceHeaderRepository}：只管理页内布局读写，不分配页、不决定 slot 生命周期。
 *
 * <p>cached 栈只保存 first pageNo，segmentId/inodeSlot 由恢复期从 first page 重建并与 FSP inode 交叉校验。
 * v3 history base 与 active/cache 目录同页，因此 commit append 与 purge unlink 可在同一 MTR 中原子改写。
 */
public final class RollbackSegmentHeaderRepository {

    /** 兼容旧测试/嵌入式调用的默认每类缓存容量；生产组合根始终显式传 EngineConfig 值。 */
    private static final int DEFAULT_CACHE_CAPACITY_PER_KIND = 8;

    /** 受控页来源；只经 MTR 取 page3 的 PageGuard。 */
    private final BufferPool pool;
    /** 实例页大小；用于校验 slot array 不越过页尾。 */
    private final PageSize pageSize;

    /**
     * 创建 {@code RollbackSegmentHeaderRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentHeaderRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("rseg header repository pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /** undo 表空间内 rseg header 固定页。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code headerPage} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static PageId headerPage(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        return PageId.of(spaceId, PageNo.of(RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO));
    }

    /**
     * 格式化空 rseg header 页（page3）：写 RSEG_HEADER 信封、固定 shape、空 history base/事务号高水位，
     * 再把 active slots 与双 cache arrays 全部置空。shape 必须能整体装入页尾 trailer 之前。
     *
     * <p>经 {@code newPage} 清零并产 PAGE_INIT redo，再经写字段产 PAGE_BYTES redo——page3 物理 crash-safe。
     * undo 表空间创建与 truncate rebuild 都在同一 MTR 内调用本方法。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param rsegId 参与 {@code format} 的稳定领域标识 {@code RollbackSegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param cacheCapacityPerKind 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void format(MiniTransaction mtr, SpaceId spaceId, RollbackSegmentId rsegId,
                       int slotCapacity, int cacheCapacityPerKind) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (rsegId == null) {
            throw new DatabaseValidationException("rollback segment id must not be null");
        }
        RollbackSegmentHeaderCapacity.validate(pageSize, slotCapacity, cacheCapacityPerKind);
        PageId pageId = headerPage(spaceId);
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.RSEG_HEADER);
        PageEnvelope.writeHeader(g, new FilePageHeader(spaceId, RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO,
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.RSEG_HEADER));
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.MAGIC,
                RollbackSegmentHeaderLayout.MAGIC_VALUE, "format rseg header magic");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.FORMAT,
                RollbackSegmentHeaderLayout.FORMAT_VERSION, "format rseg header version");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.RSEG_ID,
                rsegId.value(), "format rseg header id");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.SLOT_CAPACITY,
                slotCapacity, "format rseg slot capacity");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.CACHE_CAPACITY_PER_KIND,
                cacheCapacityPerKind, "format rseg cache capacity");
        writeCacheCount(mtr, g, rsegId.value(), UndoLogKind.INSERT, 0);
        writeCacheCount(mtr, g, rsegId.value(), UndoLogKind.UPDATE, 0);
        writeHistoryLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.HISTORY_HEAD_PAGE_NO,
                FilePageHeader.FIL_NULL, "format empty rseg history head");
        writeHistoryLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.HISTORY_TAIL_PAGE_NO,
                FilePageHeader.FIL_NULL, "format empty rseg history tail");
        writeHistoryLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.HISTORY_LENGTH,
                0L, "format empty rseg history length");
        writeHistoryLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.LAST_TRANSACTION_NO,
                0L, "format rseg transaction number high-water");
        writeFreeBaseLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.FREE_HEAD_PAGE_NO,
                FilePageHeader.FIL_NULL, "format empty rseg free head");
        writeFreeBaseLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.FREE_TAIL_PAGE_NO,
                FilePageHeader.FIL_NULL, "format empty rseg free tail");
        writeFreeBaseLong(mtr, g, rsegId.value(), RollbackSegmentHeaderLayout.FREE_LENGTH,
                0L, "format empty rseg free length");
        for (int i = 0; i < slotCapacity; i++) {
            UndoRedoDeltas.writeLong(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_SLOT,
                    rsegId.value(), i, RollbackSegmentHeaderLayout.slotOffset(i),
                    FilePageHeader.FIL_NULL, "format empty rseg slot");
        }
        for (UndoLogKind kind : List.of(UndoLogKind.INSERT, UndoLogKind.UPDATE)) {
            for (int i = 0; i < cacheCapacityPerKind; i++) {
                writeCachePageNo(mtr, g, rsegId.value(), kind, slotCapacity,
                        cacheCapacityPerKind, i, FilePageHeader.FIL_NULL, "format empty rseg cache entry");
            }
        }
    }

    /** 兼容旧四参调用；新生产代码应显式传入持久 cache 容量，owner 字段始终按 v4 初始化。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param rsegId 参与 {@code format} 的稳定领域标识 {@code RollbackSegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    public void format(MiniTransaction mtr, SpaceId spaceId, RollbackSegmentId rsegId, int slotCapacity) {
        format(mtr, spaceId, rsegId, slotCapacity, DEFAULT_CACHE_CAPACITY_PER_KIND);
    }

    /**
     * 以 compare-and-set 语义认领一个磁盘空 slot。只有当前值为 {@code FIL_NULL} 才写入 first page；重复 claim
     * 或内存/磁盘 owner 漂移必须在覆盖旧 owner 前 fail-closed。
     *
     * @param mtr       持有 page3 X latch 并收集 redo 的短 MTR。
     * @param spaceId   rseg page3 所属 undo 表空间。
     * @param slot      要认领的稳定 slot 下标。
     * @param firstPage 新 undo segment 首页，必须属于同一 undo tablespace。
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void claimSlot(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot, PageId firstPage) {
        SlotWrite write = prepareSlotWrite(mtr, spaceId, slot, firstPage, "claim");
        long current = write.guard().readLong(write.offset());
        if (current != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("claim of occupied rseg slot " + slot.value()
                    + ": current first page=" + current);
        }
        writeSlotPageNo(mtr, write, firstPage.pageNo().value(), "claim rseg slot first page");
    }

    /**
     * 在物理 undo segment 分配前以 page3 S latch 预检槽为空。方法只读取页格式、容量和目标 slot，且无论成功或
     * 异常都在返回前释放 latch，使后续 FSP page0/page2 分配不形成 page3 -&gt; FSP 的逆序等待。持久 claim 仍须
     * 在创建段后用 {@link #claimSlot} 再做一次 X-latch CAS；本方法只把正常单进程冲突前移到无物理副作用阶段。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr     当前业务 MTR；调用方必须在触碰其他页前调用本方法。
     * @param spaceId page3 所属 undo 表空间。
     * @param slot    已由内存 claim lease 预留的稳定槽号。
     * @throws UndoSlotOwnershipConflictException 磁盘槽已有 owner 时抛出。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void requireSlotFree(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        requireSpace(spaceId);
        if (slot == null) {
            throw new DatabaseValidationException("rseg slot preflight slot must not be null");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        PageId pageId = headerPage(spaceId);
        PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.SHARED);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        try {
            HeaderShape shape = validateHeaderAndReadShape(guard);
            int capacity = shape.slotCapacity();
            int idx = slot.value();
            if (idx < 0 || idx >= capacity) {
                throw new UndoLogFormatException("rseg slot out of range: " + idx + " capacity=" + capacity);
            }
            long current = guard.readLong(RollbackSegmentHeaderLayout.slotOffset(idx));
            if (current != FilePageHeader.FIL_NULL) {
                throw new UndoSlotOwnershipConflictException("preflight of occupied rseg slot " + idx
                        + ": current first page=" + current);
            }
        } finally {
            mtr.releaseLatch(pageId, guard);
        }
    }

    /**
     * 以 compare-and-set 语义清空 slot。只有磁盘 owner 精确等于 {@code expectedFirstPage} 才写 {@code FIL_NULL}，
     * 防止 stale rollback/purge 清掉已经被后续事务复用的 slot。
     *
     * @param mtr               finalization MTR；FSP drop 已在同一 MTR 中先执行。
     * @param spaceId           rseg page3 所属 undo 表空间。
     * @param slot              要清理的稳定 slot 下标。
     * @param expectedFirstPage 调用方预期仍占有该 slot 的 undo segment 首页。
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void clearSlot(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot, PageId expectedFirstPage) {
        SlotWrite write = prepareSlotWrite(mtr, spaceId, slot, expectedFirstPage, "clear");
        long current = write.guard().readLong(write.offset());
        long expected = expectedFirstPage.pageNo().value();
        if (current != expected) {
            throw new UndoLogFormatException("clear of stale rseg slot " + slot.value()
                    + ": expected first page=" + expected + ", current=" + current);
        }
        writeSlotPageNo(mtr, write, FilePageHeader.FIL_NULL, "clear rseg slot first page");
    }

    /** 校验公共参数、页格式与 slot 边界，并返回仍持 X latch 的写定位。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param slot 参与 {@code prepareSlotWrite} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param firstPage 参与 {@code prepareSlotWrite} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param operation 传给 {@code prepareSlotWrite} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code prepareSlotWrite} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private SlotWrite prepareSlotWrite(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot,
                                       PageId firstPage, String operation) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (slot == null || firstPage == null) {
            throw new DatabaseValidationException("rseg slot " + operation + " slot/first page must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (!firstPage.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("rseg slot first page must be in the undo space: " + firstPage);
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int capacity = shape.slotCapacity();
        int idx = slot.value();
        if (idx < 0 || idx >= capacity) {
            throw new UndoLogFormatException("rseg slot out of range: " + idx + " capacity=" + capacity);
        }
        long rsegId = shape.rsegId();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new SlotWrite(guard, idx, RollbackSegmentHeaderLayout.slotOffset(idx), rsegId);
    }

    /** 写 slot pageNo after-image；调用方已经完成 expected-owner CAS。 */
    private void writeSlotPageNo(MiniTransaction mtr, SlotWrite write, long pageNo, String reason) {
        UndoRedoDeltas.writeLong(mtr, write.guard(), write.guard().pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                write.rsegId(), write.slotIndex(), write.offset(), pageNo, reason);
    }

    /** 一次已校验 slot 写的页定位；生命周期绑定到调用方 MTR memo。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param slotIndex 参与 {@code 构造} 的零基位置 {@code slotIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param rsegId 参与 {@code 构造} 的原始数值身份 {@code rsegId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    private record SlotWrite(PageGuard guard, int slotIndex, int offset, long rsegId) {
    }

    /**
     * finalization 中一条 active slot → cached stack transition。expectedCacheCount 来自内存 cache lease，
     * repository 会与 page3 当前 count 交叉校验，防止 stale finalizer 覆盖并发 push/pop。
     *
     * @param activeSlot 参与 {@code 构造} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedFirstPage 参与 {@code 构造} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param expectedCacheCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    public record CachePush(UndoSlotId activeSlot, PageId expectedFirstPage,
                            UndoLogKind kind, int expectedCacheCount) {
        public CachePush {
            if (activeSlot == null || expectedFirstPage == null || kind == null) {
                throw new DatabaseValidationException("rseg cache push fields must not be null");
            }
            if (kind == UndoLogKind.TEMPORARY || expectedCacheCount < 0) {
                throw new DatabaseValidationException("invalid rseg cache push kind/count");
            }
        }
    }

    /** truncate 单批从一个 kind 栈顶移除的 pageNo 列表；列表顺序必须是栈顶到栈底。
     *
     * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param expectedCacheCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param topFirstPages 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    public record CacheTopRemoval(UndoLogKind kind, int expectedCacheCount,
                                  List<PageId> topFirstPages) {
        public CacheTopRemoval {
            if (kind == null || topFirstPages == null || topFirstPages.isEmpty()
                    || topFirstPages.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException("rseg cache removal fields must not be empty");
            }
            if (kind == UndoLogKind.TEMPORARY || expectedCacheCount < topFirstPages.size()) {
                throw new DatabaseValidationException("invalid rseg cache removal kind/count");
            }
            topFirstPages = List.copyOf(topFirstPages);
        }
    }

    /** finalization 中一条 active slot → free FIFO transition。页链接由同一 MTR 的 first-page batch 写入。
     *
     * @param activeSlot 参与 {@code 构造} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedFirstPage 参与 {@code 构造} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public record FreePush(UndoSlotId activeSlot, PageId expectedFirstPage) {
        public FreePush {
            if (activeSlot == null || expectedFirstPage == null) {
                throw new DatabaseValidationException("rseg free push fields must not be null");
            }
        }
    }

    /**
     * 把一条或两条 active owner 依次尾插持久 free FIFO。方法先验证 page3 base、全部 slot owner 与重复页，
     * 再统一改写 base 和 slots；普通 undo 首页链接必须由调用方在同一 MTR 中完成。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expected 参与 {@code moveActiveSlotsToFree} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param pushes 参与 {@code moveActiveSlotsToFree} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code moveActiveSlotsToFree} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentFreeListBase moveActiveSlotsToFree(MiniTransaction mtr, SpaceId spaceId,
                                                              RollbackSegmentFreeListBase expected,
                                                              List<FreePush> pushes) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expected == null || pushes == null || pushes.isEmpty() || pushes.size() > 2
                || pushes.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("rseg free pushes must contain one or two items");
        }
        if ((long) pushes.size() + expected.length() > Integer.MAX_VALUE) {
            throw new UndoLogFormatException("rseg free-list runtime length would overflow");
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        HeaderShape shape = validateHeaderAndReadShape(guard);
        requireFreeBase(guard, spaceId, expected);
        Set<Integer> slots = new HashSet<>();
        Set<Long> pageNos = new HashSet<>();
        for (FreePush push : pushes) {
            requirePageInSpace(spaceId, push.expectedFirstPage(), "free push");
            int slot = push.activeSlot().value();
            long pageNo = push.expectedFirstPage().pageNo().value();
            if (slot < 0 || slot >= shape.slotCapacity() || !slots.add(slot) || !pageNos.add(pageNo)) {
                throw new UndoLogFormatException("duplicate/out-of-range rseg free push target");
            }
            if (guard.readLong(RollbackSegmentHeaderLayout.slotOffset(slot)) != pageNo) {
                throw new UndoLogFormatException("free push active owner mismatch: slot=" + slot
                        + ", expected=" + pageNo);
            }
            requireNotInCachedStacks(guard, shape, pageNo);
            requireNotFreeEndpoint(expected, pageNo);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        PageId newHead = expected.headPageId().orElse(pushes.getFirst().expectedFirstPage());
        PageId newTail = pushes.getLast().expectedFirstPage();
        RollbackSegmentFreeListBase next = new RollbackSegmentFreeListBase(
                Optional.of(newHead), Optional.of(newTail), expected.length() + pushes.size());
        writeFreeBase(mtr, guard, shape.rsegId(), next);
        for (FreePush push : pushes) {
            UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                    shape.rsegId(), push.activeSlot().value(),
                    RollbackSegmentHeaderLayout.slotOffset(push.activeSlot().value()),
                    FilePageHeader.FIL_NULL, "move active rseg slot owner into free list");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return next;
    }

    /**
     * 以 page3 base CAS 语义把 free head 移入预留 active slot。successor 必须与冻结 length 一致；首页摘链与激活
     * 由调用方在同一业务 MTR 中完成。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expected 参与 {@code moveFreeHeadToActiveSlot} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param expectedHead 参与 {@code moveFreeHeadToActiveSlot} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param successor 可选的 {@code successor}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param activeSlot 参与 {@code moveFreeHeadToActiveSlot} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code moveFreeHeadToActiveSlot} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentFreeListBase moveFreeHeadToActiveSlot(MiniTransaction mtr, SpaceId spaceId,
                                                                 RollbackSegmentFreeListBase expected,
                                                                 PageId expectedHead,
                                                                 Optional<PageId> successor,
                                                                 UndoSlotId activeSlot) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expected == null || expectedHead == null || successor == null || activeSlot == null
                || expected.length() <= 0 || !expected.headPageId().equals(Optional.of(expectedHead))) {
            throw new DatabaseValidationException("invalid rseg free-to-active transition");
        }
        requirePageInSpace(spaceId, expectedHead, "free reuse");
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        successor.ifPresent(page -> requirePageInSpace(spaceId, page, "free reuse successor"));
        if ((expected.length() == 1) != successor.isEmpty()) {
            throw new DatabaseValidationException("free successor does not match expected length");
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        requireFreeBase(guard, spaceId, expected);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int slot = activeSlot.value();
        if (slot < 0 || slot >= shape.slotCapacity()
                || guard.readLong(RollbackSegmentHeaderLayout.slotOffset(slot)) != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("free reuse target active slot is not free: " + slot);
        }
        RollbackSegmentFreeListBase next = expected.length() == 1
                ? RollbackSegmentFreeListBase.empty()
                : new RollbackSegmentFreeListBase(successor, expected.tailPageId(), expected.length() - 1);
        writeFreeBase(mtr, guard, shape.rsegId(), next);
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                shape.rsegId(), slot, RollbackSegmentHeaderLayout.slotOffset(slot),
                expectedHead.pageNo().value(), "move free undo owner into active rseg slot");
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return next;
    }

    /**
     * truncate 在已 drop 队首 segment 后按冻结顺序缩短 free FIFO。remainingHead 是未被 drop 的新队首；其 prev
     * 清理由调用方在同一 MTR、page3 之后执行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expected 参与 {@code removeFreeHeads} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param removedHeadFirst 参与 {@code removeFreeHeads} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param remainingHead 可选的 {@code remainingHead}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @return {@code removeFreeHeads} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentFreeListBase removeFreeHeads(MiniTransaction mtr, SpaceId spaceId,
                                                       RollbackSegmentFreeListBase expected,
                                                       List<PageId> removedHeadFirst,
                                                       Optional<PageId> remainingHead) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expected == null || removedHeadFirst == null || removedHeadFirst.isEmpty()
                || removedHeadFirst.stream().anyMatch(java.util.Objects::isNull)
                || remainingHead == null || removedHeadFirst.size() > expected.length()
                || !expected.headPageId().equals(Optional.of(removedHeadFirst.getFirst()))) {
            throw new DatabaseValidationException("invalid rseg free-head removal");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        removedHeadFirst.forEach(page -> requirePageInSpace(spaceId, page, "free drain"));
        if (removedHeadFirst.stream().distinct().count() != removedHeadFirst.size()
                || ((long) removedHeadFirst.size() == expected.length()) != remainingHead.isEmpty()) {
            throw new DatabaseValidationException("rseg free-head removal closure is inconsistent");
        }
        remainingHead.ifPresent(page -> requirePageInSpace(spaceId, page, "free drain remaining head"));
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        HeaderShape shape = validateHeaderAndReadShape(guard);
        requireFreeBase(guard, spaceId, expected);
        RollbackSegmentFreeListBase next = remainingHead.isEmpty()
                ? RollbackSegmentFreeListBase.empty()
                : new RollbackSegmentFreeListBase(remainingHead, expected.tailPageId(),
                expected.length() - removedHeadFirst.size());
        writeFreeBase(mtr, guard, shape.rsegId(), next);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return next;
    }

    /**
     * 同一 finalization MTR 中把一个或两个 active owner 原子移入各自 kind 的 cached stack。方法先验证全部
     * active owner、count 和目标空槽，再开始任何 page3 写，避免 mixed rollback 只发布一半目录状态。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param pushes 参与 {@code moveActiveSlotsToCache} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void moveActiveSlotsToCache(MiniTransaction mtr, SpaceId spaceId, List<CachePush> pushes) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (pushes == null || pushes.isEmpty() || pushes.size() > 2
                || pushes.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("rseg cache pushes must contain one or two items");
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        Set<Integer> activeSlots = new HashSet<>();
        Set<UndoLogKind> kinds = new HashSet<>();
        Set<Long> pageNos = new HashSet<>();
        for (CachePush push : pushes) {
            requirePageInSpace(spaceId, push.expectedFirstPage(), "cache push");
            int slot = push.activeSlot().value();
            if (slot < 0 || slot >= shape.slotCapacity() || !activeSlots.add(slot)
                    || !kinds.add(push.kind()) || !pageNos.add(push.expectedFirstPage().pageNo().value())) {
                throw new UndoLogFormatException("duplicate/out-of-range rseg cache push target");
            }
            long activeOwner = guard.readLong(RollbackSegmentHeaderLayout.slotOffset(slot));
            if (activeOwner != push.expectedFirstPage().pageNo().value()) {
                throw new UndoLogFormatException("cache push active owner mismatch: slot=" + slot
                        + ", expected=" + push.expectedFirstPage().pageNo().value()
                        + ", current=" + activeOwner);
            }
            int count = readCacheCount(guard, shape, push.kind());
            if (count != push.expectedCacheCount() || count >= shape.cacheCapacityPerKind()) {
                throw new UndoLogFormatException("cache push count/capacity mismatch for " + push.kind()
                        + ": expected=" + push.expectedCacheCount() + ", current=" + count
                        + ", capacity=" + shape.cacheCapacityPerKind());
            }
            int targetOffset = RollbackSegmentHeaderLayout.cacheOffset(push.kind(), shape.slotCapacity(),
                    shape.cacheCapacityPerKind(), count);
            if (guard.readLong(targetOffset) != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException("cache push target entry is not empty for " + push.kind()
                        + " index=" + count);
            }
            requireNotInCachedStacks(guard, shape, push.expectedFirstPage().pageNo().value());
        }
        for (CachePush push : pushes) {
            int count = push.expectedCacheCount();
            writeCachePageNo(mtr, guard, shape.rsegId(), push.kind(), shape.slotCapacity(),
                    shape.cacheCapacityPerKind(), count, push.expectedFirstPage().pageNo().value(),
                    "push finalized undo segment into rseg cache");
            writeCacheCount(mtr, guard, shape.rsegId(), push.kind(), count + 1);
            UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                    shape.rsegId(), push.activeSlot().value(),
                    RollbackSegmentHeaderLayout.slotOffset(push.activeSlot().value()),
                    FilePageHeader.FIL_NULL, "move active rseg slot owner into cache");
        }
    }

    /**
     * 首写复用时原子弹出 cached top 并写入预留的 active slot。首页激活和 record append 随后仍在同一业务 MTR，
     * 因此 crash 只能看到旧 cached owner 或新 active owner，不会出现 FSP inode 无持久归属。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param kind 选择 {@code moveCachedTopToActiveSlot} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param expectedCacheCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param expectedFirstPage 参与 {@code moveCachedTopToActiveSlot} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param activeSlot 参与 {@code moveCachedTopToActiveSlot} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void moveCachedTopToActiveSlot(MiniTransaction mtr, SpaceId spaceId, UndoLogKind kind,
                                          int expectedCacheCount, PageId expectedFirstPage,
                                          UndoSlotId activeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requirePageInSpace(spaceId, expectedFirstPage, "cache reuse");
        if (kind == null || kind == UndoLogKind.TEMPORARY || activeSlot == null || expectedCacheCount <= 0) {
            throw new DatabaseValidationException("invalid rseg cached-to-active transition");
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        int slot = activeSlot.value();
        if (slot < 0 || slot >= shape.slotCapacity()
                || guard.readLong(RollbackSegmentHeaderLayout.slotOffset(slot)) != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("cache reuse target active slot is not free: " + slot);
        }
        int count = readCacheCount(guard, shape, kind);
        if (count != expectedCacheCount) {
            throw new UndoLogFormatException("cache reuse count changed for " + kind + ": expected="
                    + expectedCacheCount + ", current=" + count);
        }
        int top = count - 1;
        int offset = RollbackSegmentHeaderLayout.cacheOffset(kind, shape.slotCapacity(),
                shape.cacheCapacityPerKind(), top);
        long current = guard.readLong(offset);
        if (current != expectedFirstPage.pageNo().value()) {
            throw new UndoLogFormatException("cache reuse top owner mismatch for " + kind + ": expected="
                    + expectedFirstPage.pageNo().value() + ", current=" + current);
        }
        writeCachePageNo(mtr, guard, shape.rsegId(), kind, shape.slotCapacity(),
                shape.cacheCapacityPerKind(), top, FilePageHeader.FIL_NULL,
                "pop reused undo segment from rseg cache");
        writeCacheCount(mtr, guard, shape.rsegId(), kind, top);
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                shape.rsegId(), slot, RollbackSegmentHeaderLayout.slotOffset(slot),
                expectedFirstPage.pageNo().value(), "move cached undo owner into active rseg slot");
    }

    /**
     * truncate drain 在 FSP drop 后按 expected top 列表缩短一个或两个 cache 栈。全部 count/top 校验先完成，
     * 再清 entry/count；调用方以独立只读预检保证 FSP 写前已经发现可预测的目录损坏。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param removals 参与 {@code removeCachedTops} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void removeCachedTops(MiniTransaction mtr, SpaceId spaceId, List<CacheTopRemoval> removals) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (removals == null || removals.isEmpty() || removals.size() > 2
                || removals.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("rseg cache removals must contain one or two kinds");
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        Set<UndoLogKind> kinds = new HashSet<>();
        for (CacheTopRemoval removal : removals) {
            if (!kinds.add(removal.kind())) {
                throw new DatabaseValidationException("duplicate rseg cache removal kind: " + removal.kind());
            }
            int count = readCacheCount(guard, shape, removal.kind());
            if (count != removal.expectedCacheCount()) {
                throw new UndoLogFormatException("cache drain count changed for " + removal.kind()
                        + ": expected=" + removal.expectedCacheCount() + ", current=" + count);
            }
            for (int i = 0; i < removal.topFirstPages().size(); i++) {
                PageId expected = removal.topFirstPages().get(i);
                requirePageInSpace(spaceId, expected, "cache drain");
                int index = count - 1 - i;
                long current = guard.readLong(RollbackSegmentHeaderLayout.cacheOffset(removal.kind(),
                        shape.slotCapacity(), shape.cacheCapacityPerKind(), index));
                if (current != expected.pageNo().value()) {
                    throw new UndoLogFormatException("cache drain top mismatch for " + removal.kind()
                            + " index=" + index + ": expected=" + expected.pageNo().value()
                            + ", current=" + current);
                }
            }
        }
        for (CacheTopRemoval removal : removals) {
            int count = removal.expectedCacheCount();
            for (int i = 0; i < removal.topFirstPages().size(); i++) {
                int index = count - 1 - i;
                writeCachePageNo(mtr, guard, shape.rsegId(), removal.kind(), shape.slotCapacity(),
                        shape.cacheCapacityPerKind(), index, FilePageHeader.FIL_NULL,
                        "remove dropped undo segment from rseg cache");
            }
            writeCacheCount(mtr, guard, shape.rsegId(), removal.kind(),
                    count - removal.topFirstPages().size());
        }
    }

    /**
     * 读 rseg header 快照并校验：page type 必须 RSEG_HEADER、magic/format 正确、rsegId 与 slotCapacity 与
     * 期望（配置）一致——任一不符抛 {@link UndoLogFormatException}。占用槽收集为 {@code slot -> 首页}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param expectedRsegId  期望 rseg id（配置）。
     * @param expectedCapacity 期望 slot 容量（配置）。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expectedCacheCapacityPerKind 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code read} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId,
                                              RollbackSegmentId expectedRsegId, int expectedCapacity,
                                              int expectedCacheCapacityPerKind) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expectedRsegId == null) {
            throw new DatabaseValidationException("expected rollback segment id must not be null");
        }
        PageGuard g = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.SHARED);
        HeaderShape shape = validateHeaderAndReadShape(g);
        int capacity = shape.slotCapacity();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (capacity != expectedCapacity) {
            throw new UndoLogFormatException("rseg slotCapacity mismatch: disk=" + capacity
                    + " expected=" + expectedCapacity);
        }
        if (shape.cacheCapacityPerKind() != expectedCacheCapacityPerKind) {
            throw new UndoLogFormatException("rseg cache capacity mismatch: disk="
                    + shape.cacheCapacityPerKind() + " expected=" + expectedCacheCapacityPerKind);
        }
        int rsegId = g.readInt(RollbackSegmentHeaderLayout.RSEG_ID);
        if (rsegId != expectedRsegId.value()) {
            throw new UndoLogFormatException("rseg id mismatch: disk=" + rsegId
                    + " expected=" + expectedRsegId.value());
        }
        Map<UndoSlotId, PageId> occupied = new LinkedHashMap<>();
        Set<Long> owners = new HashSet<>();
        for (int i = 0; i < capacity; i++) {
            long pageNo = g.readLong(RollbackSegmentHeaderLayout.slotOffset(i));
            if (pageNo != FilePageHeader.FIL_NULL) {
                requireUniqueOwner(owners, pageNo, "active slot " + i);
                occupied.put(UndoSlotId.of(i), PageId.of(spaceId, PageNo.of(pageNo)));
            }
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        List<PageId> insert = readCacheStack(g, spaceId, shape, UndoLogKind.INSERT, owners);
        List<PageId> update = readCacheStack(g, spaceId, shape, UndoLogKind.UPDATE, owners);
        RollbackSegmentHistoryBase historyBase = readHistoryBase(g, spaceId, shape);
        RollbackSegmentFreeListBase freeListBase = readFreeBase(g, spaceId);
        if (historyBase.length() > occupied.size()
                || historyBase.headPageId().filter(page -> !occupied.containsValue(page)).isPresent()
                || historyBase.tailPageId().filter(page -> !occupied.containsValue(page)).isPresent()) {
            throw new UndoLogFormatException("rseg history endpoints/length are not covered by active slots: base="
                    + historyBase + ", activeOwners=" + occupied.size());
        }
        freeListBase.headPageId().ifPresent(page -> requireOwnerOutsideDirectories(owners, page, "free head"));
        freeListBase.tailPageId().filter(page -> !freeListBase.headPageId().equals(Optional.of(page)))
                .ifPresent(page -> requireOwnerOutsideDirectories(owners, page, "free tail"));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new RollbackSegmentHeaderSnapshot(expectedRsegId, capacity,
                shape.cacheCapacityPerKind(), occupied, insert, update, historyBase, freeListBase);
    }

    /** 兼容旧四参读取；新生产代码应使用显式 cache 容量完成 v3 shape 守门。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expectedRsegId 参与 {@code read} 的稳定领域标识 {@code RollbackSegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code read} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public RollbackSegmentHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId,
                                               RollbackSegmentId expectedRsegId, int expectedCapacity) {
        return read(mtr, spaceId, expectedRsegId, expectedCapacity, DEFAULT_CACHE_CAPACITY_PER_KIND);
    }

    /** 校验 page type/magic/format 与两个容量字段，并返回页上 v2 布局。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param g 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code validateHeaderAndReadShape} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private HeaderShape validateHeaderAndReadShape(PageGuard g) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        FilePageHeader header = PageEnvelope.readHeader(g);
        if (header.pageType() != PageType.RSEG_HEADER) {
            throw new UndoLogFormatException("rseg header page type mismatch: " + header.pageType());
        }
        int magic = g.readInt(RollbackSegmentHeaderLayout.MAGIC);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (magic != RollbackSegmentHeaderLayout.MAGIC_VALUE) {
            throw new UndoLogFormatException("rseg header magic mismatch: " + Integer.toHexString(magic));
        }
        int format = g.readInt(RollbackSegmentHeaderLayout.FORMAT);
        if (format != RollbackSegmentHeaderLayout.FORMAT_VERSION) {
            throw new UndoLogFormatException("rseg header format mismatch: " + format);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int capacity = g.readInt(RollbackSegmentHeaderLayout.SLOT_CAPACITY);
        int cacheCapacity = g.readInt(RollbackSegmentHeaderLayout.CACHE_CAPACITY_PER_KIND);
        try {
            RollbackSegmentHeaderCapacity.validate(pageSize, capacity, cacheCapacity);
        } catch (DatabaseValidationException invalid) {
            throw new UndoLogFormatException("invalid rseg header capacity: slots=" + capacity
                    + ", cachePerKind=" + cacheCapacity, invalid);
        }
        long rsegId = g.readInt(RollbackSegmentHeaderLayout.RSEG_ID) & 0xFFFFFFFFL;
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new HeaderShape(capacity, cacheCapacity, rsegId);
    }

    /**
     * 定位并读取Undo 日志领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param shape 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param kind 选择 {@code readCacheStack} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param owners 参与 {@code readCacheStack} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private List<PageId> readCacheStack(PageGuard guard, SpaceId spaceId, HeaderShape shape,
                                        UndoLogKind kind, Set<Long> owners) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        int count = readCacheCount(guard, shape, kind);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        List<PageId> result = new ArrayList<>(count);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        for (int i = 0; i < shape.cacheCapacityPerKind(); i++) {
            long pageNo = guard.readLong(RollbackSegmentHeaderLayout.cacheOffset(kind,
                    shape.slotCapacity(), shape.cacheCapacityPerKind(), i));
            if (i < count) {
                if (pageNo == FilePageHeader.FIL_NULL) {
                    throw new UndoLogFormatException(kind + " cache stack contains a hole at " + i);
                }
                requireUniqueOwner(owners, pageNo, kind + " cache entry " + i);
                result.add(PageId.of(spaceId, PageNo.of(pageNo)));
            } else if (pageNo != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException(kind + " cache tail entry must be empty at " + i
                        + ": current=" + pageNo);
            }
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return List.copyOf(result);
    }

    /**
     * 在已持 page3 latch 时读取并校验 v3 history base。这里只校验页内结构；节点闭包由 recovery 遍历 first page
     * 后交叉验证，避免 page3 latch 下再获取普通 undo 页 latch。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param shape 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @return {@code readHistoryBase} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private RollbackSegmentHistoryBase readHistoryBase(PageGuard guard, SpaceId spaceId, HeaderShape shape) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        long head = guard.readLong(RollbackSegmentHeaderLayout.HISTORY_HEAD_PAGE_NO);
        long tail = guard.readLong(RollbackSegmentHeaderLayout.HISTORY_TAIL_PAGE_NO);
        long length = guard.readLong(RollbackSegmentHeaderLayout.HISTORY_LENGTH);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        long lastTransactionNo = guard.readLong(RollbackSegmentHeaderLayout.LAST_TRANSACTION_NO);
        if (length < 0 || length > shape.slotCapacity() || lastTransactionNo < 0) {
            throw new UndoLogFormatException("invalid rseg history length/high-water: length=" + length
                    + ", lastTransactionNo=" + lastTransactionNo + ", slots=" + shape.slotCapacity());
        }
        boolean emptyEndpoints = head == FilePageHeader.FIL_NULL && tail == FilePageHeader.FIL_NULL;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if ((head == FilePageHeader.FIL_NULL) != (tail == FilePageHeader.FIL_NULL)
                || (length == 0) != emptyEndpoints) {
            throw new UndoLogFormatException("rseg history endpoints/length are inconsistent: head=" + head
                    + ", tail=" + tail + ", length=" + length);
        }
        if (!emptyEndpoints && (head < 0 || tail < 0
                || head == RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO
                || tail == RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO)) {
            throw new UndoLogFormatException("invalid rseg history endpoint: head=" + head + ", tail=" + tail);
        }
        Optional<PageId> headId = emptyEndpoints ? Optional.empty()
                : Optional.of(PageId.of(spaceId, PageNo.of(head)));
        Optional<PageId> tailId = emptyEndpoints ? Optional.empty()
                : Optional.of(PageId.of(spaceId, PageNo.of(tail)));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new RollbackSegmentHistoryBase(headId, tailId, length, TransactionNo.of(lastTransactionNo));
    }

    /** 在 page3 latch 下读取 v4 free base；节点闭包和 FSP 资格由 recovery 在短 MTR 中逐节点验证。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private RollbackSegmentFreeListBase readFreeBase(PageGuard guard, SpaceId spaceId) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        long head = guard.readLong(RollbackSegmentHeaderLayout.FREE_HEAD_PAGE_NO);
        long tail = guard.readLong(RollbackSegmentHeaderLayout.FREE_TAIL_PAGE_NO);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        long length = guard.readLong(RollbackSegmentHeaderLayout.FREE_LENGTH);
        boolean emptyEndpoints = head == FilePageHeader.FIL_NULL && tail == FilePageHeader.FIL_NULL;
        if (length < 0 || length > Integer.MAX_VALUE
                || (head == FilePageHeader.FIL_NULL) != (tail == FilePageHeader.FIL_NULL)
                || (length == 0) != emptyEndpoints) {
            throw new UndoLogFormatException("rseg free endpoints/length are inconsistent: head=" + head
                    + ", tail=" + tail + ", length=" + length);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (!emptyEndpoints && (head < 0 || tail < 0
                || head == RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO
                || tail == RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO)) {
            throw new UndoLogFormatException("invalid rseg free endpoint: head=" + head + ", tail=" + tail);
        }
        Optional<PageId> headId = emptyEndpoints ? Optional.empty()
                : Optional.of(PageId.of(spaceId, PageNo.of(head)));
        Optional<PageId> tailId = emptyEndpoints ? Optional.empty()
                : Optional.of(PageId.of(spaceId, PageNo.of(tail)));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new RollbackSegmentFreeListBase(headId, tailId, length);
    }

    /**
     * 在 commit MTR 中把 UPDATE undo first page 追加到持久 history 尾部。调用方须先完成 first-page/旧尾预检；
     * 本方法在任何写入前再次核对 page3 base 与 active slot owner，防止并发 finalizer 覆盖彼此的链端点。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expected 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param activeSlot 参与 {@code appendHistory} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param newTail 参与 {@code appendHistory} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param transactionNo 参与 {@code appendHistory} 的稳定领域标识 {@code TransactionNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code appendHistory} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentHistoryBase appendHistory(MiniTransaction mtr, SpaceId spaceId,
                                                     RollbackSegmentHistoryBase expected,
                                                     UndoSlotId activeSlot, PageId newTail,
                                                     TransactionNo transactionNo) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expected == null || activeSlot == null || newTail == null || transactionNo == null
                || transactionNo.isNone()) {
            throw new DatabaseValidationException("rseg history append arguments are invalid");
        }
        requirePageInSpace(spaceId, newTail, "history append");
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        RollbackSegmentHistoryBase current = readHistoryBase(guard, spaceId, shape);
        if (!current.equals(expected)) {
            throw new UndoLogFormatException("rseg history base changed before append: expected="
                    + expected + ", current=" + current);
        }
        int slot = activeSlot.value();
        if (slot < 0 || slot >= shape.slotCapacity()
                || guard.readLong(RollbackSegmentHeaderLayout.slotOffset(slot)) != newTail.pageNo().value()) {
            throw new UndoLogFormatException("history append active owner mismatch: slot=" + slot
                    + ", firstPage=" + newTail);
        }
        if (current.length() >= shape.slotCapacity()) {
            throw new UndoLogFormatException("rseg history length exceeds active slot capacity");
        }
        PageId head = current.headPageId().orElse(newTail);
        RollbackSegmentHistoryBase updated = new RollbackSegmentHistoryBase(Optional.of(head), Optional.of(newTail),
                current.length() + 1L,
                TransactionNo.of(Math.max(current.lastTransactionNo().value(), transactionNo.value())));
        writeHistoryBase(mtr, guard, shape.rsegId(), updated);
        return updated;
    }

    /**
     * 在 purge finalization MTR 中摘除持久 history 队首。lastTransactionNo 是提交号高水位，摘链时保持不变。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expected 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param activeSlot 参与 {@code removeHistoryHead} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param removedHead 参与 {@code removeHistoryHead} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param newHead 可选的 {@code newHead}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @return {@code removeHistoryHead} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollbackSegmentHistoryBase removeHistoryHead(MiniTransaction mtr, SpaceId spaceId,
                                                         RollbackSegmentHistoryBase expected,
                                                         UndoSlotId activeSlot, PageId removedHead,
                                                         Optional<PageId> newHead) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expected == null || activeSlot == null || removedHead == null || newHead == null
                || expected.length() <= 0 || !expected.headPageId().equals(Optional.of(removedHead))) {
            throw new DatabaseValidationException("rseg history head removal arguments are invalid");
        }
        requirePageInSpace(spaceId, removedHead, "history head removal");
        newHead.ifPresent(page -> requirePageInSpace(spaceId, page, "history new head"));
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        HeaderShape shape = validateHeaderAndReadShape(guard);
        RollbackSegmentHistoryBase current = readHistoryBase(guard, spaceId, shape);
        if (!current.equals(expected)) {
            throw new UndoLogFormatException("rseg history base changed before head removal: expected="
                    + expected + ", current=" + current);
        }
        int slot = activeSlot.value();
        if (slot < 0 || slot >= shape.slotCapacity()
                || guard.readLong(RollbackSegmentHeaderLayout.slotOffset(slot)) != removedHead.pageNo().value()) {
            throw new UndoLogFormatException("history removal active owner mismatch: slot=" + slot
                    + ", firstPage=" + removedHead);
        }
        long nextLength = current.length() - 1L;
        if ((nextLength == 0) != newHead.isEmpty()) {
            throw new UndoLogFormatException("history removal new head is inconsistent with remaining length");
        }
        Optional<PageId> newTail = nextLength == 0 ? Optional.empty() : current.tailPageId();
        RollbackSegmentHistoryBase updated = new RollbackSegmentHistoryBase(newHead, newTail, nextLength,
                current.lastTransactionNo());
        writeHistoryBase(mtr, guard, shape.rsegId(), updated);
        return updated;
    }

    /** 把已验证的 history base 写成四个独立物理 redo after-image；MTR commit 是其原子可见边界。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param rsegId 参与 {@code writeHistoryBase} 的原始数值身份 {@code rsegId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param base 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     */
    private static void writeHistoryBase(MiniTransaction mtr, PageGuard guard, long rsegId,
                                         RollbackSegmentHistoryBase base) {
        writeHistoryLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.HISTORY_HEAD_PAGE_NO,
                base.headPageId().map(p -> p.pageNo().value()).orElse(FilePageHeader.FIL_NULL),
                "write rseg history head");
        writeHistoryLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.HISTORY_TAIL_PAGE_NO,
                base.tailPageId().map(p -> p.pageNo().value()).orElse(FilePageHeader.FIL_NULL),
                "write rseg history tail");
        writeHistoryLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.HISTORY_LENGTH,
                base.length(), "write rseg history length");
        writeHistoryLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.LAST_TRANSACTION_NO,
                base.lastTransactionNo().value(), "write rseg transaction number high-water");
    }

    private static void writeHistoryLong(MiniTransaction mtr, PageGuard guard, long rsegId,
                                         int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_HISTORY_BASE,
                rsegId, 0, offset, value, reason);
    }

    /**
     * 计算 {@code readCacheCount} 所表达的Undo 日志数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param shape 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param kind 选择 {@code readCacheCount} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code readCacheCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private int readCacheCount(PageGuard guard, HeaderShape shape, UndoLogKind kind) {
        int count = guard.readInt(cacheCountOffset(kind));
        if (count < 0 || count > shape.cacheCapacityPerKind()) {
            throw new UndoLogFormatException(kind + " cache count out of range: " + count
                    + " capacity=" + shape.cacheCapacityPerKind());
        }
        return count;
    }

    private void requireNotInCachedStacks(PageGuard guard, HeaderShape shape, long pageNo) {
        for (UndoLogKind kind : List.of(UndoLogKind.INSERT, UndoLogKind.UPDATE)) {
            int count = readCacheCount(guard, shape, kind);
            for (int i = 0; i < count; i++) {
                long current = guard.readLong(RollbackSegmentHeaderLayout.cacheOffset(kind,
                        shape.slotCapacity(), shape.cacheCapacityPerKind(), i));
                if (current == pageNo) {
                    throw new UndoLogFormatException("active owner is already present in " + kind
                            + " cache at index " + i + ": " + pageNo);
                }
            }
        }
    }

    /** 比较调用方冻结的 free base 与 page3 当前权威值；任何漂移都必须在写 owner 前失败。 */
    private void requireFreeBase(PageGuard guard, SpaceId spaceId, RollbackSegmentFreeListBase expected) {
        RollbackSegmentFreeListBase current = readFreeBase(guard, spaceId);
        if (!current.equals(expected)) {
            throw new UndoLogFormatException("rseg free base changed: expected=" + expected + ", current=" + current);
        }
    }

    /**
     * 校验 {@code requireNotFreeEndpoint} 涉及的Undo 日志结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param base 参与 {@code requireNotFreeEndpoint} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param pageNo 参与 {@code requireNotFreeEndpoint} 的原始数值身份 {@code pageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static void requireNotFreeEndpoint(RollbackSegmentFreeListBase base, long pageNo) {
        if (base.headPageId().filter(page -> page.pageNo().value() == pageNo).isPresent()
                || base.tailPageId().filter(page -> page.pageNo().value() == pageNo).isPresent()) {
            throw new UndoLogFormatException("active owner is already a free-list endpoint: " + pageNo);
        }
    }

    private static void requireOwnerOutsideDirectories(Set<Long> owners, PageId page, String location) {
        requireUniqueOwner(owners, page.pageNo().value(), location);
    }

    private static void requireUniqueOwner(Set<Long> owners, long pageNo, String location) {
        if (pageNo < 0 || !owners.add(pageNo)) {
            throw new UndoLogFormatException("duplicate/invalid rseg owner at " + location + ": " + pageNo);
        }
    }

    private static void requirePageInSpace(SpaceId spaceId, PageId pageId, String operation) {
        if (pageId == null || !pageId.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException(operation + " first page must belong to undo space");
        }
    }

    private static int cacheCountOffset(UndoLogKind kind) {
        if (kind == null) {
            throw new DatabaseValidationException("rseg cache kind must not be null");
        }
        return switch (kind) {
            case INSERT -> RollbackSegmentHeaderLayout.INSERT_CACHE_COUNT;
            case UPDATE -> RollbackSegmentHeaderLayout.UPDATE_CACHE_COUNT;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no persistent cache count");
        };
    }

    private static void writeCacheCount(MiniTransaction mtr, PageGuard guard, long rsegId,
                                        UndoLogKind kind, int count) {
        UndoRedoDeltas.writeInt(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_CACHE_COUNT,
                rsegId, kind.ordinal(), cacheCountOffset(kind), count,
                "write " + kind + " rseg cache count");
    }

    private static void writeCachePageNo(MiniTransaction mtr, PageGuard guard, long rsegId,
                                         UndoLogKind kind, int slotCapacity, int cacheCapacityPerKind,
                                         int index, long pageNo, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_CACHE_ENTRY,
                rsegId, index, RollbackSegmentHeaderLayout.cacheOffset(kind, slotCapacity,
                cacheCapacityPerKind, index), pageNo, reason + " (" + kind + ")");
    }

    /** 同一页内依次写 head/tail/length；MTR redo batch 保证 crash 后只观察完整 committed owner 迁移。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param rsegId 参与 {@code writeFreeBase} 的原始数值身份 {@code rsegId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param base 参与 {@code writeFreeBase} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private static void writeFreeBase(MiniTransaction mtr, PageGuard guard, long rsegId,
                                      RollbackSegmentFreeListBase base) {
        writeFreeBaseLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.FREE_HEAD_PAGE_NO,
                base.headPageId().map(page -> page.pageNo().value()).orElse(FilePageHeader.FIL_NULL),
                "write rseg free head");
        writeFreeBaseLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.FREE_TAIL_PAGE_NO,
                base.tailPageId().map(page -> page.pageNo().value()).orElse(FilePageHeader.FIL_NULL),
                "write rseg free tail");
        writeFreeBaseLong(mtr, guard, rsegId, RollbackSegmentHeaderLayout.FREE_LENGTH,
                base.length(), "write rseg free length");
    }

    private static void writeFreeBaseLong(MiniTransaction mtr, PageGuard guard, long rsegId,
                                          int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.RSEG_FREE_BASE,
                rsegId, 0, offset, value, reason);
    }

    /** 已校验 page3 v4 的动态布局。
     *
     * @param slotCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param cacheCapacityPerKind 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param rsegId 参与 {@code 构造} 的原始数值身份 {@code rsegId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    private record HeaderShape(int slotCapacity, int cacheCapacityPerKind, long rsegId) {
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
