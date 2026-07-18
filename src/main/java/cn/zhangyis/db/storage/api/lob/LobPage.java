package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/** LOB 页格式化/读取访问器；所有校验都在持目标页 latch 时完成，不跨页保存 PageGuard。 */
final class LobPage {

    private LobPage() {
    }

    /**
     * 在已由 PAGE_INIT(BLOB) 创建的页中写入完整 body 和 FIL 链链接。整页写均被 MTR collector 收集为 PAGE_BYTES。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param previousPageNo 参与 {@code format} 的原始数值身份 {@code previousPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code format} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param chunkIndex 参与 {@code format} 的零基位置 {@code chunkIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param segmentId 参与 {@code format} 的稳定领域标识 {@code SegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param inodeSlot 参与 {@code format} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param totalLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param wholeCrc32 参与 {@code format} 的位域或校验值 {@code wholeCrc32}；只允许当前格式定义的位，数值按无符号位模式解释
     * @param chunk 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void format(PageGuard guard, PageId pageId, long previousPageNo, long nextPageNo,
                       int chunkIndex, int pageCount, SegmentId segmentId, int inodeSlot,
                       int totalLength, long wholeCrc32, byte[] chunk) {
        if (guard == null || pageId == null || segmentId == null || chunk == null) {
            throw new DatabaseValidationException("LOB page format arguments must not be null");
        }
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                previousPageNo, nextPageNo, 0L, PageType.BLOB));
        guard.writeInt(LobPageLayout.MAGIC, LobPageLayout.MAGIC_VALUE);
        guard.writeInt(LobPageLayout.VERSION, LobPageLayout.VERSION_VALUE);
        guard.writeInt(LobPageLayout.CHUNK_INDEX, chunkIndex);
        guard.writeInt(LobPageLayout.CHUNK_LENGTH, chunk.length);
        guard.writeLong(LobPageLayout.SEGMENT_ID, segmentId.value());
        guard.writeInt(LobPageLayout.INODE_SLOT, inodeSlot);
        guard.writeInt(LobPageLayout.TOTAL_LENGTH, totalLength);
        guard.writeInt(LobPageLayout.WHOLE_CRC32, (int) wholeCrc32);
        guard.writeInt(LobPageLayout.PAGE_COUNT, pageCount);
        guard.writeBytes(LobPageLayout.DATA, chunk);
    }

    /** 读取并校验单页局部格式；跨页 index/link/CRC 不变量由 LobStorage 结合 reference 检查。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param expectedPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param payloadCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code read} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws LobPageCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static Snapshot read(PageGuard guard, PageId expectedPageId, int payloadCapacity) {
        try {
            // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
            FilePageHeader header = PageEnvelope.readHeader(guard);
            if (!header.spaceId().equals(expectedPageId.spaceId())
                    || header.pageNo() != expectedPageId.pageNo().value()
                    || header.pageType() != PageType.BLOB) {
                throw new LobPageCorruptedException("LOB page envelope mismatch at " + expectedPageId
                        + ": " + header);
            }
            int magic = guard.readInt(LobPageLayout.MAGIC);
            int version = guard.readInt(LobPageLayout.VERSION);
            // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
            int chunkIndex = guard.readInt(LobPageLayout.CHUNK_INDEX);
            int chunkLength = guard.readInt(LobPageLayout.CHUNK_LENGTH);
            long segmentId = guard.readLong(LobPageLayout.SEGMENT_ID);
            int inodeSlot = guard.readInt(LobPageLayout.INODE_SLOT);
            // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
            int totalLength = guard.readInt(LobPageLayout.TOTAL_LENGTH);
            long crc32 = guard.readInt(LobPageLayout.WHOLE_CRC32) & 0xFFFF_FFFFL;
            int pageCount = guard.readInt(LobPageLayout.PAGE_COUNT);
            if (magic != LobPageLayout.MAGIC_VALUE || version != LobPageLayout.VERSION_VALUE) {
                throw new LobPageCorruptedException("LOB magic/version mismatch at " + expectedPageId);
            }
            if (chunkIndex < 0 || chunkLength <= 0 || chunkLength > payloadCapacity
                    || segmentId <= 0 || inodeSlot < 0 || totalLength <= 0 || pageCount <= 0) {
                throw new LobPageCorruptedException("LOB body bounds invalid at " + expectedPageId);
            }
            // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
            return new Snapshot(header.prevPageNo(), header.nextPageNo(), chunkIndex, pageCount,
                    SegmentId.of(segmentId), inodeSlot, totalLength, crc32,
                    guard.readBytes(LobPageLayout.DATA, chunkLength));
        } catch (LobPageCorruptedException corrupted) {
            throw corrupted;
        } catch (RuntimeException decodeFailure) {
            throw new LobPageCorruptedException("cannot decode LOB page " + expectedPageId, decodeFailure);
        }
    }

    /** 已校验的单页快照；byte[] 只在当前 LOB 操作内部传递。
     *
     * @param previousPageNo 参与 {@code 构造} 的原始数值身份 {@code previousPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code 构造} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param chunkIndex 参与 {@code 构造} 的零基位置 {@code chunkIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param segmentId 参与 {@code 构造} 的稳定领域标识 {@code SegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param inodeSlot 参与 {@code 构造} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param totalLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param wholeCrc32 参与 {@code 构造} 的位域或校验值 {@code wholeCrc32}；只允许当前格式定义的位，数值按无符号位模式解释
     * @param chunk 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    record Snapshot(long previousPageNo, long nextPageNo, int chunkIndex, int pageCount,
                    SegmentId segmentId, int inodeSlot, int totalLength, long wholeCrc32, byte[] chunk) {
    }
}
