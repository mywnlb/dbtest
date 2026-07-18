package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrSavepoint;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * External undo payload 页链的物理读写协作者。写路径只操作调用方已经预留容量的同一 undo segment；读路径逐页复制
 * chunk 后回滚到 MTR savepoint，避免长版本链读取同时 pin 全部 payload frame。
 */
final class UndoPayloadStorage {

    /**
     * 本对象持有的 {@code pool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool pool;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 本对象持有的 {@code tablespaceRegistry} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceRegistry tablespaceRegistry;

    /**
     * 创建 {@code UndoPayloadStorage}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param tablespaceRegistry 由组合根提供的 {@code TablespaceRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoPayloadStorage(BufferPool pool, PageSize pageSize, TablespaceRegistry tablespaceRegistry) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("undo payload storage pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.tablespaceRegistry = tablespaceRegistry;
    }

    /** 分配、格式化完整页链并返回尚未发布到普通 UNDO 页的根描述符。
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
     * @param allocator 由组合根提供的 {@code UndoSpaceAllocator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code write} 调用
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code write} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoPayloadDescriptor write(MiniTransaction mtr, UndoSpaceAllocator allocator, UndoSegmentHandle handle,
                                UndoRecordWritePlan plan) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || allocator == null || handle == null || plan == null || !plan.external()) {
            throw new DatabaseValidationException("external undo payload write args/mode invalid");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        byte[] payload = plan.encodedPayloadUnsafe();
        int capacity = UndoPayloadPageLayout.payloadCapacity(pageSize);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        List<PageId> pages = new ArrayList<>(plan.externalPageCount());
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "external undo allocation: fresh same-segment pages are unreachable before root descriptor")) {
            for (int i = 0; i < plan.externalPageCount(); i++) {
                pages.add(allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId()));
            }
            for (int i = 0; i < pages.size(); i++) {
                int offset = i * capacity;
                int length = Math.min(capacity, payload.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(payload, offset, chunk, 0, length);
                PageId pageId = pages.get(i);
                requireOrdinaryAccess(mtr, pageId.spaceId());
                PageGuard guard = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO_PAYLOAD);
                long previous = i == 0 ? FilePageHeader.FIL_NULL : pages.get(i - 1).pageNo().value();
                long next = i + 1 == pages.size() ? FilePageHeader.FIL_NULL : pages.get(i + 1).pageNo().value();
                UndoPayloadPage.format(guard, pageId, previous, next, i, handle,
                        plan.record().transactionId(), plan.record().undoNo(), payload.length,
                        pages.size(), plan.crc32(), chunk);
            }
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new UndoPayloadDescriptor(plan.record().type(), plan.record().transactionId(),
                plan.record().undoNo(), pages.get(0).pageNo(), payload.length, pages.size(), plan.crc32());
    }

    /**
     * prepared append 阶段固定全部 external payload 页并取得 X guard，但不写 placeholder body。新页此时仍不可达；
     * root descriptor 只会在 {@link #writePrepared(UndoSegmentHandle, UndoRecordWritePlan, List)} 完成真实 body 后发布。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param allocator 由组合根提供的 {@code UndoSpaceAllocator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code preparePages} 调用
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    List<PageId> preparePages(MiniTransaction mtr, UndoSpaceAllocator allocator, UndoSegmentHandle handle,
                              int pageCount) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || allocator == null || handle == null || pageCount <= 0) {
            throw new DatabaseValidationException("prepared undo payload page arguments are invalid");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        List<PageId> pages = new ArrayList<>(pageCount);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "prepared external undo pages are fresh and unreachable; FSP never waits for undo page latches")) {
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId());
                requireOrdinaryAccess(mtr, pageId.spaceId());
                mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO_PAYLOAD);
                pages.add(pageId);
            }
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return List.copyOf(pages);
    }

    /**
     * 把真实 undo payload 写入 prepare 阶段已经固定的页。页数与 retained X guard 必须精确匹配；任何偏差表示
     * prepared shape 被破坏，必须在 root descriptor/record slot 发布前拒绝。
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
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param plan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param pages 参与 {@code writePrepared} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code writePrepared} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoPayloadDescriptor writePrepared(MiniTransaction mtr, UndoSegmentHandle handle,
                                        UndoRecordWritePlan plan, List<PageId> pages) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || handle == null || plan == null || pages == null || !plan.external()
                || pages.size() != plan.externalPageCount()) {
            throw new DatabaseValidationException("prepared undo payload shape mismatch");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        byte[] payload = plan.encodedPayloadUnsafe();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int capacity = UndoPayloadPageLayout.payloadCapacity(pageSize);
        for (int i = 0; i < pages.size(); i++) {
            PageId pageId = pages.get(i);
            PageGuard guard = mtr.retainedExclusivePage(pageId);
            if (guard == null) {
                throw new UndoLogFormatException("prepared undo payload page lost its X guard: " + pageId);
            }
            int offset = i * capacity;
            int length = Math.min(capacity, payload.length - offset);
            byte[] chunk = new byte[length];
            System.arraycopy(payload, offset, chunk, 0, length);
            long previous = i == 0 ? FilePageHeader.FIL_NULL : pages.get(i - 1).pageNo().value();
            long next = i + 1 == pages.size() ? FilePageHeader.FIL_NULL : pages.get(i + 1).pageNo().value();
            UndoPayloadPage.format(guard, pageId, previous, next, i, handle,
                    plan.record().transactionId(), plan.record().undoNo(), payload.length,
                    pages.size(), plan.crc32(), chunk);
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new UndoPayloadDescriptor(plan.record().type(), plan.record().transactionId(),
                plan.record().undoNo(), pages.getFirst().pageNo(), payload.length, pages.size(), plan.crc32());
    }


    /** 严格读取 descriptor 指向的页链；任何 link、owner、长度、页数或 CRC 不一致都拒绝返回部分记录。
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
     * @param owner 表空间文件或 segment 的稳定身份与生命周期快照；不得为 {@code null}，必须与已打开文件和当前 generation 一致
     * @param descriptor 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param maxExternalPages 参与 {@code read} 的上界或规格值 {@code maxExternalPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code read} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoPayloadTooLargeException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    byte[] read(MiniTransaction mtr, SpaceId spaceId, SegmentIdentity owner,
                UndoPayloadDescriptor descriptor, int maxExternalPages) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || spaceId == null || owner == null || descriptor == null) {
            throw new DatabaseValidationException("external undo payload read args must not be null");
        }
        if (descriptor.pageCount() > maxExternalPages) {
            throw new UndoPayloadTooLargeException("stored external undo payload uses " + descriptor.pageCount()
                    + " pages, configured maximum is " + maxExternalPages);
        }
        int capacity = UndoPayloadPageLayout.payloadCapacity(pageSize);
        int canonicalPages = (int) (((long) descriptor.totalLength() + capacity - 1L) / capacity);
        if (canonicalPages != descriptor.pageCount()) {
            throw new UndoLogFormatException("external undo descriptor non-canonical page count: expected="
                    + canonicalPages + " actual=" + descriptor.pageCount());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        ByteArrayOutputStream out = new ByteArrayOutputStream(descriptor.totalLength());
        long pageNo = descriptor.firstPageNo().value();
        long expectedPrevious = FilePageHeader.FIL_NULL;
        Set<Long> visited = new HashSet<>(descriptor.pageCount());
        for (int index = 0; index < descriptor.pageCount(); index++) {
            if (pageNo >= FilePageHeader.FIL_NULL || !visited.add(pageNo)) {
                throw new UndoLogFormatException("external undo payload has premature tail/invalid/cyclic page at index "
                        + index + ": " + pageNo);
            }
            PageId pageId = PageId.of(spaceId, PageNo.of(pageNo));
            UndoPayloadPage.Snapshot page;
            PageGuard retained = mtr.retainedExclusivePage(pageId);
            if (retained != null) {
                page = UndoPayloadPage.read(retained, pageId, capacity);
            } else {
                MtrSavepoint savepoint = mtr.savepoint();
                try (var ignored = mtr.allowOutOfOrderPageLatch(
                        "external undo read follows validated logical links, independent of physical page order")) {
                    requireOrdinaryAccess(mtr, spaceId);
                    PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.SHARED);
                    page = UndoPayloadPage.read(guard, pageId, capacity);
                } finally {
                    mtr.rollbackToSavepoint(savepoint);
                }
            }
            validatePage(pageId, page, owner, descriptor, index, expectedPrevious);
            int expectedChunk = Math.min(capacity, descriptor.totalLength() - out.size());
            if (page.chunk().length != expectedChunk) {
                throw new UndoLogFormatException("external undo chunk length mismatch at " + pageId
                        + ": expected=" + expectedChunk + " actual=" + page.chunk().length);
            }
            out.writeBytes(page.chunk());
            expectedPrevious = pageNo;
            pageNo = page.nextPageNo();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (pageNo != FilePageHeader.FIL_NULL || out.size() != descriptor.totalLength()) {
            throw new UndoLogFormatException("external undo payload chain tail/length mismatch");
        }
        byte[] payload = out.toByteArray();
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        if (crc32.getValue() != descriptor.crc32()) {
            throw new UndoLogFormatException("external undo payload CRC mismatch");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return payload;
    }

    private static void validatePage(PageId pageId, UndoPayloadPage.Snapshot page, SegmentIdentity owner,
                                     UndoPayloadDescriptor descriptor, int index, long expectedPrevious) {
        boolean tailLinkValid = index + 1 < descriptor.pageCount()
                ? page.nextPageNo() < FilePageHeader.FIL_NULL
                : page.nextPageNo() == FilePageHeader.FIL_NULL;
        if (page.previousPageNo() != expectedPrevious || !tailLinkValid || page.chunkIndex() != index
                || !page.segmentId().equals(owner.segmentId()) || page.inodeSlot() != owner.inodeSlot()
                || !page.transactionId().equals(descriptor.transactionId())
                || !page.undoNo().equals(descriptor.undoNo())
                || page.totalLength() != descriptor.totalLength() || page.pageCount() != descriptor.pageCount()
                || page.wholeCrc32() != descriptor.crc32()) {
            throw new UndoLogFormatException("external undo payload page identity/link mismatch at " + pageId);
        }
    }

    private void requireOrdinaryAccess(MiniTransaction mtr, SpaceId spaceId) {
        if (tablespaceRegistry != null) {
            mtr.acquireTablespaceLease(spaceId);
            tablespaceRegistry.require(spaceId);
        }
    }

    /** 根 UNDO 页提供的 segment 所有权，避免 direct RollPointer 读取依赖 first-page handle。
     *
     * @param segmentId 参与 {@code 构造} 的稳定领域标识 {@code cn.zhangyis.db.domain.SegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param inodeSlot 参与 {@code 构造} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     */
    record SegmentIdentity(cn.zhangyis.db.domain.SegmentId segmentId, int inodeSlot) {
        SegmentIdentity {
            if (segmentId == null || segmentId.value() <= 0 || inodeSlot < 0) {
                throw new DatabaseValidationException("invalid external undo segment identity");
            }
        }
    }
}
