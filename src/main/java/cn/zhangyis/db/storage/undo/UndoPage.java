package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;

import java.nio.ByteBuffer;

/**
 * undo 页视图（PageGuard 之上）。所有状态都在页字节中，写入经 MTR 持有的 X latch guard 产生
 * PAGE_BYTES redo，commit 时由 MTR 盖 pageLSN。
 *
 * <p>本类只表达页内物理格式：page header 每页都有，log header 仅 first 页完整可读写；v3 chain 页在其余
 * 预留字节清零后复制 kind，既避免旧字节误解释，也支持 direct RollPointer 类型守门。
 */
public final class UndoPage {

    /**
     * 当前物理短事务。UndoPage 不拥有提交/回滚，只用它把页头/log header after-image 追加成 undo metadata redo。
     */
    private final MiniTransaction mtr;

    /**
     * MTR-owned 页访问 guard。UndoPage 不拥有释放职责，生命周期由 MiniTransaction commit/rollback 统一释放。
     */
    private final PageGuard guard;

    /**
     * 页大小用于追加时计算 FIL trailer 前的写入上界，防止 undo record 覆盖校验/LSN trailer 区域。
     */
    private final PageSize pageSize;

    /**
     * 创建 {@code UndoPage}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoPage(MiniTransaction mtr, PageGuard guard, PageSize pageSize) {
        if (mtr == null || guard == null || pageSize == null) {
            throw new DatabaseValidationException("undo page mtr/guard/pageSize must not be null");
        }
        this.mtr = mtr;
        this.guard = guard;
        this.pageSize = pageSize;
    }

    /**
     * 格式化 undo log first 页。数据流：写每页 page header（含 segment 归属和 first 标志）→ 写 log header
     * 的事务、类型、状态与链端点 → 初始化整链计数。调用方必须已通过 {@link UndoPageAccess} 写好 UNDO 页信封。
     *
     * @param kind   独立 undo log 类型；普通表空间只使用 INSERT/UPDATE。
     * @param txnId  所属事务 id；恢复用它把 ACTIVE/COMMITTED first page 与事务证据交叉校验。
     * @param handle segment 定位；first 页必须位于 handle 所属表空间。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void formatFirstPage(UndoLogKind kind, TransactionId txnId, UndoSegmentHandle handle) {
        if (kind == null || txnId == null || handle == null) {
            throw new DatabaseValidationException("undo first page format args must not be null");
        }
        requireHandleSpace(handle);
        rewriteFirstPage(kind, txnId, UndoPageLayout.STATE_ACTIVE, handle, "format undo first page");
    }

    /**
     * 把已结束且只有一个普通页的 segment 重置为 CACHED。record area 不全页清零：freeOffset 回到起点后旧槽
     * 不再可达，下一次激活/append 会从头覆盖；这是教学实现相对 InnoDB 更简单的缓存清理策略。
     *
     * @param kind 选择 {@code resetForCache} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void resetForCache(UndoLogKind kind, UndoSegmentHandle handle) {
        if (kind == null || handle == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("undo cache reset kind/handle is invalid");
        }
        requireFirstPage();
        requireHandleSpace(handle);
        if (!guard.pageId().equals(handle.firstPageId()) || !handle.firstPageId().equals(handle.lastPageId())
                || undoKind() != kind || lastPageNo() != guard.pageId().pageNo().value()
                || prevPageNo() != FilePageHeader.FIL_NULL || nextPageNo() != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("only a self-linked single-page undo segment can enter cache: "
                    + guard.pageId());
        }
        rewriteFirstPage(kind, TransactionId.NONE, UndoPageLayout.STATE_CACHED, handle,
                "reset finalized undo first page for cache");
    }

    /** 用新的事务 owner 激活 CACHED 首页；调用方随后在同一业务 MTR 追加首条 undo record。
     *
     * @param kind 选择 {@code activateCached} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param txnId 参与 {@code activateCached} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void activateCached(UndoLogKind kind, TransactionId txnId, UndoSegmentHandle handle) {
        if (kind == null || txnId == null || handle == null || txnId.isNone()
                || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("undo cache activation args are invalid");
        }
        requireCachedEmpty(kind, handle);
        rewriteFirstPage(kind, txnId, UndoPageLayout.STATE_ACTIVE, handle,
                "activate cached undo first page");
    }

    /**
     * 把已结束且物理上只有一个 ordinary page 的 segment 重置为 FREE，并写入 free FIFO 双向链接。
     * 最近一次 kind 只留作诊断；FREE owner 本身不按 kind 分区。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param previousFreePageNo 参与 {@code resetForFree} 的原始数值身份 {@code previousFreePageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextFreePageNo 参与 {@code resetForFree} 的原始数值身份 {@code nextFreePageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void resetForFree(UndoSegmentHandle handle, long previousFreePageNo, long nextFreePageNo) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (handle == null) {
            throw new DatabaseValidationException("undo free reset handle must not be null");
        }
        requireFirstPage();
        requireHandleSpace(handle);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        validateFreePageNo(previousFreePageNo);
        validateFreePageNo(nextFreePageNo);
        if (!guard.pageId().equals(handle.firstPageId()) || !handle.firstPageId().equals(handle.lastPageId())
                || lastPageNo() != guard.pageId().pageNo().value()
                || prevPageNo() != FilePageHeader.FIL_NULL || nextPageNo() != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("only a self-linked single-page undo segment can enter free list: "
                    + guard.pageId());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoLogKind retainedKind = undoKind();
        if (retainedKind == UndoLogKind.TEMPORARY) {
            throw new UndoLogFormatException("temporary undo cannot enter persistent free list: " + guard.pageId());
        }
        rewriteFirstPage(retainedKind, TransactionId.NONE, UndoPageLayout.STATE_FREE, handle,
                "reset finalized undo first page for free list");
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        setFreeLinks(previousFreePageNo, nextFreePageNo);
    }

    /** 用新事务和新 kind 激活 FREE 首页；调用方随后在同一业务 MTR 追加首条记录。
     *
     * @param kind 选择 {@code activateFree} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param txnId 参与 {@code activateFree} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void activateFree(UndoLogKind kind, TransactionId txnId, UndoSegmentHandle handle) {
        if (kind == null || txnId == null || handle == null || txnId.isNone()
                || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("undo free activation args are invalid");
        }
        requireFreeEmpty(handle);
        rewriteFirstPage(kind, txnId, UndoPageLayout.STATE_ACTIVE, handle,
                "activate free undo first page");
    }

    /** 校验 FREE 首页的可复用物理边界；prev/next 可以指向同一持久 FIFO 中的相邻节点。
     *
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void requireFreeEmpty(UndoSegmentHandle handle) {
        requireFirstPage();
        requireHandleSpace(handle);
        if (state() != UndoPageLayout.STATE_FREE || !transactionId().isNone() || commitNo() != 0L
                || undoKind() == UndoLogKind.TEMPORARY
                || freeOffset() != UndoPageLayout.RECORD_AREA_START || recordCount() != 0
                || pageLastUndoNo().value() != 0L || logRecordCount() != 0L
                || logLastUndoNo().value() != 0L || !logicalHead().isEmpty()
                || !guard.pageId().equals(handle.firstPageId()) || !handle.firstPageId().equals(handle.lastPageId())
                || firstPageNo() != guard.pageId().pageNo().value()
                || lastPageNo() != guard.pageId().pageNo().value()
                || prevPageNo() != FilePageHeader.FIL_NULL || nextPageNo() != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("free undo first page is not an empty single-page owner: "
                    + guard.pageId());
        }
        validateFreePageNo(freePrevPageNo());
        validateFreePageNo(freeNextPageNo());
    }

    /** 校验 CACHED 首页的全部可复用边界；恢复与运行期 pop 共用。
     *
     * @param expectedKind 选择 {@code requireCachedEmpty} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void requireCachedEmpty(UndoLogKind expectedKind, UndoSegmentHandle handle) {
        requireFirstPage();
        requireHandleSpace(handle);
        if (expectedKind == null || expectedKind == UndoLogKind.TEMPORARY
                || state() != UndoPageLayout.STATE_CACHED || undoKind() != expectedKind
                || !transactionId().isNone() || commitNo() != 0L
                || freeOffset() != UndoPageLayout.RECORD_AREA_START || recordCount() != 0
                || pageLastUndoNo().value() != 0L || logRecordCount() != 0L
                || logLastUndoNo().value() != 0L || !logicalHead().isEmpty()
                || historyPrevPageNo() != FilePageHeader.FIL_NULL
                || historyNextPageNo() != FilePageHeader.FIL_NULL
                || !guard.pageId().equals(handle.firstPageId()) || !handle.firstPageId().equals(handle.lastPageId())
                || firstPageNo() != guard.pageId().pageNo().value()
                || lastPageNo() != guard.pageId().pageNo().value()
                || prevPageNo() != FilePageHeader.FIL_NULL || nextPageNo() != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("cached undo first page is not an empty single-page owner: "
                    + guard.pageId());
        }
    }

    /**
     * 校验输入与当前状态后修改Undo 日志领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param kind 选择 {@code rewriteFirstPage} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param txnId 参与 {@code rewriteFirstPage} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param reason 传给 {@code rewriteFirstPage} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    private void rewriteFirstPage(UndoLogKind kind, TransactionId txnId, int state,
                                  UndoSegmentHandle handle, String reason) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        writePageHeader(handle, true);
        PageEnvelope.writeSiblingLinks(guard, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
        writeLogHeaderLong(handle, UndoPageLayout.TRANSACTION_ID, txnId.value(), reason + " transaction id");
        writeLogHeaderU8(handle, UndoPageLayout.UNDO_KIND, kind.ordinal(), reason + " kind");
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        writeLogHeaderU8(handle, UndoPageLayout.STATE, state, reason + " state");
        long self = guard.pageId().pageNo().value();
        writeLogHeaderU32(handle, UndoPageLayout.FIRST_PAGE_NO, self, reason + " first page no");
        writeLogHeaderU32(handle, UndoPageLayout.LAST_PAGE_NO, self, reason + " last page no");
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        writeLogHeaderLong(handle, UndoPageLayout.LOG_RECORD_COUNT, 0L, reason + " log record count");
        writeLogHeaderLong(handle, UndoPageLayout.LOG_LAST_UNDO_NO, 0L, reason + " log last undo no");
        writeLogHeaderLong(handle, UndoPageLayout.COMMIT_NO, 0L, reason + " commit no");
        setLogicalHead(UndoLogicalHead.EMPTY);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        setHistoryLinks(FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
    }

    /**
     * 格式化 undo chain 页。chain 页预留 log header 宽度，先清零 {@code [63,136)} 再写入复制的 kind，
     * 使 record area 起点与 first 页一致；其余 first-only 访问器仍通过 {@link #isFirstPage()} 拒绝。
     *
     * @param handle segment 定位；页必须与 handle 所属表空间一致。
     * @param kind 选择 {@code formatChainPage} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void formatChainPage(UndoLogKind kind, UndoSegmentHandle handle) {
        if (kind == null || handle == null) {
            throw new DatabaseValidationException("undo chain page format kind/handle must not be null");
        }
        requireHandleSpace(handle);
        writePageHeader(handle, false);
        UndoRedoDeltas.withUndoCategory(mtr, "clear undo chain page log header reservation",
                () -> guard.writeBytes(UndoPageLayout.TRANSACTION_ID,
                        new byte[UndoPageLayout.LOG_HEADER_END - UndoPageLayout.TRANSACTION_ID]));
        writeLogHeaderU8(handle, UndoPageLayout.UNDO_KIND, kind.ordinal(),
                "copy undo log kind to chain page");
    }

    /**
     * 追加一条页内 undo record。数据流：校验 undoNo 合法 → 在任何写入前做页内容量判断 →
     * 写 record 槽 {@code [len u16][payload]} 并登记 payload logical redo →
     * 推进本页 header 的 free/count/pageLastUndoNo。
     *
     * <p>本方法只维护 page header，不更新 first 页 log header；整链计数由 {@link UndoLogSegment} 在 append
     * 成功后统一更新，从而能覆盖单页与跨页两种路径。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param payload 已编码 undo record payload。
     * @param txnId   生成该 undo record 的事务写 id，用于 redo 诊断和后续恢复阶段关联。
     * @param undoNo  事务内 undo 序号，不能是 NONE。
     * @return record 槽起始 offset，供 RollPointer 编码。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoPageOverflowException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    int appendRecord(byte[] payload, TransactionId txnId, UndoNo undoNo) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (payload == null || txnId == null || undoNo == null) {
            throw new DatabaseValidationException("undo append payload/txnId/undoNo must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo append undoNo must be > 0 (not NONE)");
        }
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        int need = 2 + payload.length;
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        if (free + need > limit) {
            throw new UndoPageOverflowException("undo record (" + need + "B) does not fit at free="
                    + free + " limit=" + limit);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoRedoDeltas.writeRecordPayload(mtr, guard, guard.pageId(), txnId, undoNo, free, payload,
                "append undo record payload");
        writePageHeaderU16(UndoPageLayout.FREE_OFFSET, free + need, "advance undo page free offset");
        writePageHeaderU16(UndoPageLayout.RECORD_COUNT, getU16(UndoPageLayout.RECORD_COUNT) + 1,
                "advance undo page record count");
        writePageHeaderLong(UndoPageLayout.PAGE_LAST_UNDO_NO, undoNo.value(), "advance undo page last undo no");
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return free;
    }

    /**
     * 读取指定 offset 的 undo record payload。offset 必须落在 {@link UndoPageLayout#RECORD_AREA_START}
     * 与当前 freeOffset 之间，且 len 前缀不能越过已写区域；越界表示 roll pointer 或页内容损坏。
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code recordAt} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    byte[] recordAt(int offset) {
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        if (offset < UndoPageLayout.RECORD_AREA_START || offset + 2 > free) {
            throw new UndoLogFormatException("undo record offset out of area: " + offset + " free=" + free);
        }
        int len = getU16(offset);
        if (offset + 2 + len > free) {
            throw new UndoLogFormatException("undo record length out of area: off=" + offset + " len=" + len);
        }
        return guard.readBytes(offset + 2, len);
    }

    /**
     * 写 FIL NEXT 链接并保留现有 PREV。生长时先把当前尾页 next 指向新页，再在新页写 prev，避免覆盖 pageLSN
     * 或 pageType 等信封字段。
     *
     * @param next 新后继页号。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void linkNextTo(PageNo next) {
        if (next == null) {
            throw new DatabaseValidationException("undo link next must not be null");
        }
        long prev = PageEnvelope.readHeader(guard).prevPageNo();
        PageEnvelope.writeSiblingLinks(guard, prev, next.value());
    }

    /**
     * 写 FIL PREV 链接并保留现有 NEXT。用于新 chain 页接入前驱页，NEXT 初始保持 FIL_NULL。
     *
     * @param prev 新前驱页号。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void linkPrevTo(PageNo prev) {
        if (prev == null) {
            throw new DatabaseValidationException("undo link prev must not be null");
        }
        long next = PageEnvelope.readHeader(guard).nextPageNo();
        PageEnvelope.writeSiblingLinks(guard, prev.value(), next);
    }

    /**
     * 推进 first 页 log header 中的链尾页号。只能在 first 页调用；chain 页调用说明调用方把页角色弄错，
     * 属物理格式使用错误，按格式异常处理。
     *
     * @param last 参与 {@code setLastPageNo} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void setLastPageNo(PageNo last) {
        requireFirstPage();
        if (last == null) {
            throw new DatabaseValidationException("undo last page no must not be null");
        }
        writeLogHeaderU32(UndoPageLayout.LAST_PAGE_NO, last.value(), "advance undo log last page no");
    }

    /**
     * 更新 first 页整链 record 总数。该值是 reopen 和遍历诊断的权威整链计数，不等同于任一页的
     * {@link #recordCount()}。
     * @param count 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void setLogRecordCount(long count) {
        requireFirstPage();
        if (count < 0) {
            throw new DatabaseValidationException("undo log record count must be non-negative: " + count);
        }
        writeLogHeaderLong(UndoPageLayout.LOG_RECORD_COUNT, count, "advance undo log record count");
    }

    /**
     * 更新 first 页整链最后 undoNo。成功 append 后由 UndoLogSegment 调用；失败路径不应提前写此字段。
     *
     * @param undoNo 参与 {@code setLogLastUndoNo} 的原始数值身份 {@code undoNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void setLogLastUndoNo(long undoNo) {
        requireFirstPage();
        if (undoNo < 0) {
            throw new DatabaseValidationException("undo log last undo no must be non-negative: " + undoNo);
        }
        writeLogHeaderLong(UndoPageLayout.LOG_LAST_UNDO_NO, undoNo, "advance undo log last undo no");
    }

    /** 当前页 id。 */
    PageId pageId() {
        return guard.pageId();
    }

    /** 本页所属 UNDO segment id。 */
    SegmentId segmentId() {
        return SegmentId.of(guard.readLong(UndoPageLayout.SEGMENT_ID));
    }

    /** 本页所属 segment inode 槽。 */
    int inodeSlot() {
        return guard.readInt(UndoPageLayout.INODE_SLOT);
    }

    /** 是否为 undo log first 页；只有 first 页的 log header 有语义。 */
    boolean isFirstPage() {
        return (getU8(UndoPageLayout.PAGE_FLAGS) & UndoPageLayout.FLAG_FIRST_PAGE) != 0;
    }

    /** 原始 page flags，供格式诊断测试使用。 */
    int pageFlags() {
        return getU8(UndoPageLayout.PAGE_FLAGS);
    }

    /** 当前 undo 页物理格式版本；first 标志不参与版本值。 */
    int formatVersion() {
        return (pageFlags() & UndoPageLayout.FORMAT_VERSION_MASK) >>> UndoPageLayout.FORMAT_VERSION_SHIFT;
    }

    /**
     * 打开既有页时统一执行版本守门。v1/v2 的 record area 分别从 105/120 开始，若按当前 136 偏移继续解析会把
     * 旧 record 误当 history header 或跳过，因此 legacy/未知版本都必须快速失败，不能猜测兼容。
     *
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void requireCurrentFormat() {
        int version = formatVersion();
        if (version != UndoPageLayout.CURRENT_FORMAT_VERSION) {
            throw new UndoLogFormatException("unsupported undo page format version " + version
                    + " on " + guard.pageId() + " (expected " + UndoPageLayout.CURRENT_FORMAT_VERSION + ")");
        }
    }

    /** 下一条页内追加位置。 */
    int freeOffset() {
        return getU16(UndoPageLayout.FREE_OFFSET);
    }

    /** 本页 record 数。 */
    int recordCount() {
        return getU16(UndoPageLayout.RECORD_COUNT);
    }

    /** 本页最近 undoNo；整链最近 undoNo 见 {@link #logLastUndoNo()}。 */
    UndoNo pageLastUndoNo() {
        return UndoNo.of(guard.readLong(UndoPageLayout.PAGE_LAST_UNDO_NO));
    }

    /** FIL NEXT 页号；无后继时为 {@link FilePageHeader#FIL_NULL}。 */
    long nextPageNo() {
        return PageEnvelope.readHeader(guard).nextPageNo();
    }

    /** FIL PREV 页号；无前驱时为 {@link FilePageHeader#FIL_NULL}。 */
    long prevPageNo() {
        return PageEnvelope.readHeader(guard).prevPageNo();
    }

    /** first 页 log header 中的事务 id。
     *
     * @return {@code transactionId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    TransactionId transactionId() {
        requireFirstPage();
        return TransactionId.of(guard.readLong(UndoPageLayout.TRANSACTION_ID));
    }

    /** 当前普通 UNDO 页携带的 log 类型；v3 在 first/chain 页都必须可独立读取。
     *
     * @return {@code undoKind} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoLogKind undoKind() {
        int idx = getU8(UndoPageLayout.UNDO_KIND);
        UndoLogKind[] all = UndoLogKind.values();
        if (idx < 0 || idx >= all.length) {
            throw new UndoLogFormatException("undo kind ordinal out of range: " + idx);
        }
        return all[idx];
    }

    /** first 页 log header 中的 undo log 状态（ACTIVE/PREPARED/COMMITTED/CACHED/FREE）。
     *
     * @return {@code state} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    int state() {
        requireFirstPage();
        return getU8(UndoPageLayout.STATE);
    }

    /** 写 first 页 log header 状态（X，R 1.2）。commit 时标 COMMITTED，使恢复期能据此跳过已提交事务回滚。
     *
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void setLogState(int state) {
        requireFirstPage();
        if (state != UndoPageLayout.STATE_ACTIVE && state != UndoPageLayout.STATE_COMMITTED
                && state != UndoPageLayout.STATE_CACHED && state != UndoPageLayout.STATE_FREE
                && state != UndoPageLayout.STATE_PREPARED) {
            throw new DatabaseValidationException("unknown undo log state: " + state);
        }
        writeLogHeaderU8(UndoPageLayout.STATE, state, "write undo log state");
    }

    /** first 页 log header 中的提交序号 TransactionNo（R 1.3）；0 表尚未提交。
     *
     * @return {@code commitNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long commitNo() {
        requireFirstPage();
        return guard.readLong(UndoPageLayout.COMMIT_NO);
    }

    /** 写 first 页 log header 提交序号（X，R 1.3）。commit 时与 STATE=COMMITTED 同 MTR 写，恢复重建 history 用。
     *
     * @param commitNo 参与 {@code setCommitNo} 的原始数值身份 {@code commitNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setCommitNo(long commitNo) {
        requireFirstPage();
        writeLogHeaderLong(UndoPageLayout.COMMIT_NO, commitNo, "write undo commit no");
    }

    /** first 页在 rollback-segment history 双向链中的前驱 pageNo；无前驱为 FIL_NULL。
     *
     * @return {@code historyPrevPageNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long historyPrevPageNo() {
        requireFirstPage();
        return guard.readLong(UndoPageLayout.HISTORY_PREV_PAGE_NO);
    }

    /** first 页在 rollback-segment history 双向链中的后继 pageNo；无后继为 FIL_NULL。
     *
     * @return {@code historyNextPageNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long historyNextPageNo() {
        requireFirstPage();
        return guard.readLong(UndoPageLayout.HISTORY_NEXT_PAGE_NO);
    }

    /**
     * 更新 first 页的 history prev/next。该链接与 FIL sibling links 无关：FIL 链连接同一 undo segment 的页，
     * history 链连接不同事务的 UPDATE undo first page；二者不能混用。
     *
     * @param prevPageNo 参与 {@code setHistoryLinks} 的原始数值身份 {@code prevPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code setHistoryLinks} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setHistoryLinks(long prevPageNo, long nextPageNo) {
        requireFirstPage();
        validateHistoryPageNo(prevPageNo);
        validateHistoryPageNo(nextPageNo);
        writeHistoryLinkLong(UndoPageLayout.HISTORY_PREV_PAGE_NO, prevPageNo,
                "write undo history prev link");
        writeHistoryLinkLong(UndoPageLayout.HISTORY_NEXT_PAGE_NO, nextPageNo,
                "write undo history next link");
    }

    /** purge 摘头时把新 head.prev 清为 FIL_NULL。
     *
     * @param prevPageNo 参与 {@code setHistoryPrevPageNo} 的原始数值身份 {@code prevPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setHistoryPrevPageNo(long prevPageNo) {
        requireFirstPage();
        validateHistoryPageNo(prevPageNo);
        writeHistoryLinkLong(UndoPageLayout.HISTORY_PREV_PAGE_NO, prevPageNo,
                "write undo history prev link");
    }

    /** commit append 时把旧 tail.next 指向新节点。
     *
     * @param nextPageNo 参与 {@code setHistoryNextPageNo} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setHistoryNextPageNo(long nextPageNo) {
        requireFirstPage();
        validateHistoryPageNo(nextPageNo);
        writeHistoryLinkLong(UndoPageLayout.HISTORY_NEXT_PAGE_NO, nextPageNo,
                "write undo history next link");
    }

    /** FREE 状态下的前驱 pageNo；与 history prev 共用物理槽，但使用独立 redo 分类。
     *
     * @return {@code freePrevPageNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long freePrevPageNo() {
        requireFreeState();
        return guard.readLong(UndoPageLayout.HISTORY_PREV_PAGE_NO);
    }

    /** FREE 状态下的后继 pageNo；与 history next 共用物理槽，但使用独立 redo 分类。
     *
     * @return {@code freeNextPageNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long freeNextPageNo() {
        requireFreeState();
        return guard.readLong(UndoPageLayout.HISTORY_NEXT_PAGE_NO);
    }

    /** FREE 入队时一次写入 prev/next；调用方已按全局 PageId 顺序持有相关首页 X latch。
     *
     * @param previousPageNo 参与 {@code setFreeLinks} 的原始数值身份 {@code previousPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code setFreeLinks} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setFreeLinks(long previousPageNo, long nextPageNo) {
        requireFreeState();
        validateFreePageNo(previousPageNo);
        validateFreePageNo(nextPageNo);
        writeFreeLinkLong(UndoPageLayout.HISTORY_PREV_PAGE_NO, previousPageNo,
                "write undo free prev link");
        writeFreeLinkLong(UndoPageLayout.HISTORY_NEXT_PAGE_NO, nextPageNo,
                "write undo free next link");
    }

    /** 摘除 free head 后清空新 head.prev。
     *
     * @param previousPageNo 参与 {@code setFreePrevPageNo} 的原始数值身份 {@code previousPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setFreePrevPageNo(long previousPageNo) {
        requireFreeState();
        validateFreePageNo(previousPageNo);
        writeFreeLinkLong(UndoPageLayout.HISTORY_PREV_PAGE_NO, previousPageNo,
                "write undo free prev link");
    }

    /** 尾插 free node 时把旧 tail.next 指向新节点。
     *
     * @param nextPageNo 参与 {@code setFreeNextPageNo} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void setFreeNextPageNo(long nextPageNo) {
        requireFreeState();
        validateFreePageNo(nextPageNo);
        writeFreeLinkLong(UndoPageLayout.HISTORY_NEXT_PAGE_NO, nextPageNo,
                "write undo free next link");
    }

    /** first 页 log header 中的链首页号。
     *
     * @return {@code firstPageNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long firstPageNo() {
        requireFirstPage();
        return getU32(UndoPageLayout.FIRST_PAGE_NO);
    }

    /** first 页 log header 中的当前链尾页号。
     *
     * @return {@code lastPageNo} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     */
    long lastPageNo() {
        requireFirstPage();
        return getU32(UndoPageLayout.LAST_PAGE_NO);
    }

    /** first 页 log header 中的整链 record 总数。
     *
     * @return {@code logRecordCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    long logRecordCount() {
        requireFirstPage();
        long count = guard.readLong(UndoPageLayout.LOG_RECORD_COUNT);
        if (count < 0) {
            throw new UndoLogFormatException("negative undo log record count on " + guard.pageId() + ": " + count);
        }
        return count;
    }

    /** first 页 log header 中的整链最近 undoNo。
     *
     * @return {@code logLastUndoNo} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoNo logLastUndoNo() {
        requireFirstPage();
        long value = guard.readLong(UndoPageLayout.LOG_LAST_UNDO_NO);
        if (value < 0) {
            throw new UndoLogFormatException("negative undo log high-water on " + guard.pageId() + ": " + value);
        }
        return UndoNo.of(value);
    }

    /**
     * 读取 first-page header 中连续编码的持久逻辑头。磁盘负值、RollPointer 保留位或半空 pair 都属于
     * undo 格式损坏，统一包装成 {@link UndoLogFormatException}，避免 recovery 把它误判为普通调用参数错误。
     * @return {@code logicalHead} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoLogicalHead logicalHead() {
        requireFirstPage();
        try {
            UndoNo undoNo = UndoNo.of(guard.readLong(UndoPageLayout.LOGICAL_LAST_UNDO_NO));
            RollPointer pointer = RollPointer.decode(
                    guard.readBytes(UndoPageLayout.LOGICAL_HEAD_ROLL_POINTER, RollPointer.BYTES), 0);
            return new UndoLogicalHead(undoNo, pointer);
        } catch (DatabaseValidationException e) {
            throw new UndoLogFormatException("corrupt persistent logical undo head on " + guard.pageId(), e);
        }
    }

    /**
     * 把 logical undo 头作为单个 15 字节 after-image 写入 first-page header。一次 metadata delta 使 undoNo 与
     * pointer 在同一 MTR redo batch 中不可拆分；调用方必须在写前完成目标 record/高水位校验。
     *
     * @param head 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void setLogicalHead(UndoLogicalHead head) {
        requireFirstPage();
        if (head == null) {
            throw new DatabaseValidationException("undo logical head must not be null");
        }
        byte[] image = ByteBuffer.allocate(Long.BYTES + RollPointer.BYTES)
                .putLong(head.undoNo().value())
                .put(head.rollPointer().encode())
                .array();
        writeLogHeaderBytes(UndoPageLayout.LOGICAL_LAST_UNDO_NO, image,
                "write persistent logical undo head");
    }

    /**
     * 校验输入与当前状态后修改Undo 日志领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param first 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     */
    private void writePageHeader(UndoSegmentHandle handle, boolean first) {
        writePageHeaderU16(handle, UndoPageLayout.FREE_OFFSET, UndoPageLayout.RECORD_AREA_START,
                "format undo page free offset");
        writePageHeaderU16(handle, UndoPageLayout.RECORD_COUNT, 0, "format undo page record count");
        writePageHeaderLong(handle, UndoPageLayout.PAGE_LAST_UNDO_NO, 0L, "format undo page last undo no");
        writePageHeaderLong(handle, UndoPageLayout.SEGMENT_ID, handle.segmentId().value(), "format undo segment id");
        writePageHeaderU32(handle, UndoPageLayout.INODE_SLOT, handle.inodeSlot(), "format undo inode slot");
        int flags = UndoPageLayout.CURRENT_FORMAT_FLAGS | (first ? UndoPageLayout.FLAG_FIRST_PAGE : 0);
        writePageHeaderU8(handle, UndoPageLayout.PAGE_FLAGS, flags,
                "format undo page flags");
    }

    private void writePageHeaderU8(UndoSegmentHandle handle, int offset, int value, String reason) {
        UndoRedoDeltas.writeU8(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, value, reason);
    }

    private void writePageHeaderU16(UndoSegmentHandle handle, int offset, int value, String reason) {
        UndoRedoDeltas.writeU16(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, value, reason);
    }

    private void writePageHeaderU32(UndoSegmentHandle handle, int offset, long value, String reason) {
        validateU32(value);
        UndoRedoDeltas.writeInt(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, (int) value, reason);
    }

    private void writePageHeaderLong(UndoSegmentHandle handle, int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, value, reason);
    }

    private void writePageHeaderU16(int offset, int value, String reason) {
        UndoRedoDeltas.writeU16(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    private void writePageHeaderLong(int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_PAGE_HEADER_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    private void writeLogHeaderU8(UndoSegmentHandle handle, int offset, int value, String reason) {
        UndoRedoDeltas.writeU8(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, value, reason);
    }

    private void writeLogHeaderU32(UndoSegmentHandle handle, int offset, long value, String reason) {
        validateU32(value);
        UndoRedoDeltas.writeInt(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, (int) value, reason);
    }

    private void writeLogHeaderLong(UndoSegmentHandle handle, int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                handle.segmentId().value(), handle.inodeSlot(), offset, value, reason);
    }

    private void writeLogHeaderU8(int offset, int value, String reason) {
        UndoRedoDeltas.writeU8(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    private void writeLogHeaderU32(int offset, long value, String reason) {
        validateU32(value);
        UndoRedoDeltas.writeInt(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                segmentId().value(), inodeSlot(), offset, (int) value, reason);
    }

    private void writeLogHeaderLong(int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    private void writeLogHeaderBytes(int offset, byte[] value, String reason) {
        UndoRedoDeltas.writeBytes(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_LOG_HEADER_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    /** history 链接使用独立 redo 分类，恢复期可区分事务状态与跨事务节点链接。 */
    private void writeHistoryLinkLong(int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_HISTORY_LINK_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    /** FREE 链链接独立分类，避免 recovery 把状态复用字段误解释成 history 关系。 */
    private void writeFreeLinkLong(int offset, long value, String reason) {
        UndoRedoDeltas.writeLong(mtr, guard, guard.pageId(), UndoMetadataDeltaKind.UNDO_FREE_LINK_FIELD,
                segmentId().value(), inodeSlot(), offset, value, reason);
    }

    private static void validateHistoryPageNo(long pageNo) {
        if (pageNo < 0 || pageNo > FilePageHeader.FIL_NULL) {
            throw new DatabaseValidationException("undo history page no is out of range: " + pageNo);
        }
    }

    private static void validateFreePageNo(long pageNo) {
        if (pageNo < 0 || pageNo > FilePageHeader.FIL_NULL) {
            throw new DatabaseValidationException("undo free-list page no is out of range: " + pageNo);
        }
    }

    private void requireFreeState() {
        requireFirstPage();
        if (state() != UndoPageLayout.STATE_FREE) {
            throw new UndoLogFormatException("undo page is not FREE: " + guard.pageId());
        }
    }

    private void requireHandleSpace(UndoSegmentHandle handle) {
        if (!handle.spaceId().equals(guard.pageId().spaceId())) {
            throw new DatabaseValidationException("undo page " + guard.pageId()
                    + " space mismatch with handle " + handle.spaceId());
        }
    }

    private void requireFirstPage() {
        if (!isFirstPage()) {
            throw new UndoLogFormatException("undo page is not the log first page: " + guard.pageId());
        }
    }

    private int getU8(int off) {
        return guard.readBytes(off, 1)[0] & 0xFF;
    }

    private int getU16(int off) {
        byte[] b = guard.readBytes(off, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    private long getU32(int off) {
        return guard.readInt(off) & 0xFFFFFFFFL;
    }

    private void validateU32(long v) {
        if (v < 0 || v > FilePageHeader.FIL_NULL) {
            throw new DatabaseValidationException("u32 out of range: " + v);
        }
    }
}
