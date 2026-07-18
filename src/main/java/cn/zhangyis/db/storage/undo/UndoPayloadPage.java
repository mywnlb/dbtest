package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/** External undo payload 单页格式化与读取原语；跨页一致性由 {@link UndoPayloadStorage} 统一验证。 */
final class UndoPayloadPage {

    private UndoPayloadPage() {
    }

    /** 在刚 PAGE_INIT(UNDO_PAYLOAD) 的页中写入完整不可变 body 与 FIL 链链接。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param previousPageNo 参与 {@code format} 的原始数值身份 {@code previousPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code format} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param chunkIndex 参与 {@code format} 的零基位置 {@code chunkIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param undoNo 参与 {@code format} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param totalLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param wholeCrc32 参与 {@code format} 的位域或校验值 {@code wholeCrc32}；只允许当前格式定义的位，数值按无符号位模式解释
     * @param chunk 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void format(PageGuard guard, PageId pageId, long previousPageNo, long nextPageNo,
                       int chunkIndex, UndoSegmentHandle handle, TransactionId transactionId, UndoNo undoNo,
                       int totalLength, int pageCount, long wholeCrc32, byte[] chunk) {
        if (guard == null || pageId == null || handle == null || transactionId == null
                || undoNo == null || chunk == null) {
            throw new DatabaseValidationException("undo payload page format args must not be null");
        }
        if (chunkIndex < 0 || chunk.length == 0 || totalLength <= 0 || pageCount <= 0
                || chunkIndex >= pageCount || transactionId.isNone() || undoNo.isNone()
                || wholeCrc32 < 0 || wholeCrc32 > 0xFFFF_FFFFL) {
            throw new DatabaseValidationException("undo payload page format bounds invalid");
        }
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                previousPageNo, nextPageNo, 0L, PageType.UNDO_PAYLOAD));
        guard.writeInt(UndoPayloadPageLayout.MAGIC, UndoPayloadPageLayout.MAGIC_VALUE);
        guard.writeInt(UndoPayloadPageLayout.VERSION, UndoPayloadPageLayout.VERSION_VALUE);
        guard.writeInt(UndoPayloadPageLayout.CHUNK_INDEX, chunkIndex);
        guard.writeInt(UndoPayloadPageLayout.CHUNK_LENGTH, chunk.length);
        guard.writeLong(UndoPayloadPageLayout.SEGMENT_ID, handle.segmentId().value());
        guard.writeInt(UndoPayloadPageLayout.INODE_SLOT, handle.inodeSlot());
        guard.writeLong(UndoPayloadPageLayout.TRANSACTION_ID, transactionId.value());
        guard.writeLong(UndoPayloadPageLayout.UNDO_NO, undoNo.value());
        guard.writeInt(UndoPayloadPageLayout.TOTAL_LENGTH, totalLength);
        guard.writeInt(UndoPayloadPageLayout.PAGE_COUNT, pageCount);
        guard.writeInt(UndoPayloadPageLayout.WHOLE_CRC32, (int) wholeCrc32);
        guard.writeBytes(UndoPayloadPageLayout.DATA, chunk);
    }

    /** 读取并校验单页局部边界；链顺序、归属和整体 CRC 在上层聚合校验。
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
     * @param expectedPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param payloadCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code read} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static Snapshot read(PageGuard guard, PageId expectedPageId, int payloadCapacity) {
        try {
            // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
            FilePageHeader header = PageEnvelope.readHeader(guard);
            if (!header.spaceId().equals(expectedPageId.spaceId())
                    || header.pageNo() != expectedPageId.pageNo().value()
                    || header.pageType() != PageType.UNDO_PAYLOAD) {
                throw new UndoLogFormatException("external undo payload envelope mismatch at " + expectedPageId);
            }
            int magic = guard.readInt(UndoPayloadPageLayout.MAGIC);
            int version = guard.readInt(UndoPayloadPageLayout.VERSION);
            int chunkIndex = guard.readInt(UndoPayloadPageLayout.CHUNK_INDEX);
            // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
            int chunkLength = guard.readInt(UndoPayloadPageLayout.CHUNK_LENGTH);
            long segmentId = guard.readLong(UndoPayloadPageLayout.SEGMENT_ID);
            int inodeSlot = guard.readInt(UndoPayloadPageLayout.INODE_SLOT);
            long transactionId = guard.readLong(UndoPayloadPageLayout.TRANSACTION_ID);
            long undoNo = guard.readLong(UndoPayloadPageLayout.UNDO_NO);
            // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
            int totalLength = guard.readInt(UndoPayloadPageLayout.TOTAL_LENGTH);
            int pageCount = guard.readInt(UndoPayloadPageLayout.PAGE_COUNT);
            long crc32 = guard.readInt(UndoPayloadPageLayout.WHOLE_CRC32) & 0xFFFF_FFFFL;
            if (magic != UndoPayloadPageLayout.MAGIC_VALUE || version != UndoPayloadPageLayout.VERSION_VALUE) {
                throw new UndoLogFormatException("external undo payload magic/version mismatch at " + expectedPageId);
            }
            if (chunkIndex < 0 || chunkLength <= 0 || chunkLength > payloadCapacity || segmentId <= 0
                    || inodeSlot < 0 || transactionId <= 0 || undoNo <= 0 || totalLength <= 0 || pageCount <= 0) {
                throw new UndoLogFormatException("external undo payload body bounds invalid at " + expectedPageId);
            }
            // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
            return new Snapshot(header.prevPageNo(), header.nextPageNo(), chunkIndex,
                    SegmentId.of(segmentId), inodeSlot, TransactionId.of(transactionId), UndoNo.of(undoNo),
                    totalLength, pageCount, crc32,
                    guard.readBytes(UndoPayloadPageLayout.DATA, chunkLength));
        } catch (UndoLogFormatException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new UndoLogFormatException("cannot decode external undo payload page " + expectedPageId, error);
        }
    }

    /** 已复制出 PageGuard 生命周期的单页快照。
     *
     * @param previousPageNo 参与 {@code 构造} 的原始数值身份 {@code previousPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code 构造} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param chunkIndex 参与 {@code 构造} 的零基位置 {@code chunkIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param segmentId 参与 {@code 构造} 的稳定领域标识 {@code SegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param inodeSlot 参与 {@code 构造} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param undoNo 参与 {@code 构造} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param totalLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param wholeCrc32 参与 {@code 构造} 的位域或校验值 {@code wholeCrc32}；只允许当前格式定义的位，数值按无符号位模式解释
     * @param chunk 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    record Snapshot(long previousPageNo, long nextPageNo, int chunkIndex, SegmentId segmentId, int inodeSlot,
                    TransactionId transactionId, UndoNo undoNo, int totalLength, int pageCount,
                    long wholeCrc32, byte[] chunk) {
    }
}
