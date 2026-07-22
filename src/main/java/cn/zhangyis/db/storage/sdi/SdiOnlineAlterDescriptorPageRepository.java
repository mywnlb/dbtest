package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;

/**
 * `DDL_DESCRIPTOR`页物理仓储。FSP页归属由`DiskSpaceManager`先建立，本类只在同一MTR内把仍由X guard
 * 持有的ALLOCATED页格式化为descriptor，或在恢复读取时验证FIL envelope后解码body。
 */
public final class SdiOnlineAlterDescriptorPageRepository {

    /** descriptor页共享的Buffer Pool入口。 */
    private final BufferPool pool;
    /** 固定页面几何与body codec。 */
    private final PageSize pageSize;
    /** owner/entry/CRC格式的无状态codec。 */
    private final SdiOnlineAlterDescriptorPageCodec codec;

    /**
     * @param pool 与FSP/MTR共享的Buffer Pool
     * @param pageSize 当前实例唯一页大小
     */
    public SdiOnlineAlterDescriptorPageRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor repository requires pool/page size");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.codec = new SdiOnlineAlterDescriptorPageCodec(pageSize);
    }

    /**
     * 把当前MTR刚分配且仍持X guard的ALLOCATED页格式化为DDL_DESCRIPTOR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验page/model space与chain page identity，取得MTR已保留的X guard而不重复fix。</li>
     *     <li>确认FSP初始化留下的envelope为同一ALLOCATED页，拒绝覆盖既有专用页。</li>
     *     <li>重写FIL type/next link并写满含零尾的codec body；PAGE_BYTES与先前allocation intent同MTR提交。</li>
     * </ol>
     *
     * @param mtr 已执行segment page allocation的活动MTR
     * @param pageId FSP刚归属descriptor segment的新页
     * @param descriptor 完整owner、ordinal、next与entries
     * @throws SdiPageCorruptionException guard缺失或envelope不属于待格式化ALLOCATED页时抛出
     */
    public void formatAllocated(MiniTransaction mtr, PageId pageId,
                                SdiOnlineAlterDescriptorPage descriptor) {
        // 1. 不允许仓储自行分配页，也不重复调用newPage产生第二条PAGE_INIT。
        if (mtr == null || pageId == null || descriptor == null
                || !pageId.spaceId().equals(descriptor.descriptorSegment().spaceId())) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor format identity is invalid");
        }
        PageGuard page = mtr.retainedExclusivePage(pageId);
        if (page == null) {
            throw new SdiPageCorruptionException(
                    "descriptor page is not retained exclusively by allocation MTR: " + pageId);
        }

        // 2. FSP owner先于本格式存在；未知type说明调用方试图覆盖已发布数据结构。
        FilePageHeader current = PageEnvelope.readHeader(page);
        if (!current.spaceId().equals(pageId.spaceId())
                || current.pageNo() != pageId.pageNo().value()
                || current.pageType() != PageType.ALLOCATED) {
            throw new SdiPageCorruptionException(
                    "descriptor target is not the expected ALLOCATED page: " + pageId);
        }

        // 3. body先完整编码再写页；编码失败不会留下半格式化header。
        byte[] body = codec.encode(descriptor);
        PageEnvelope.writeHeader(page, new FilePageHeader(
                pageId.spaceId(), pageId.pageNo().value(), FilePageHeader.FIL_NULL,
                descriptor.nextPageNo() == 0
                        ? FilePageHeader.FIL_NULL : descriptor.nextPageNo(),
                0L, PageType.DDL_DESCRIPTOR));
        page.writeBytes(PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES, body);
    }

    /**
     * 读取一张descriptor页并交叉验证envelope type、space、page与next link。
     *
     * @param mtr 负责页S latch/fix的只读MTR
     * @param pageId anchor或前页next指向的精确页
     * @return 完整解码且与FIL next一致的descriptor页
     * @throws SdiPageCorruptionException envelope/body损坏或链指针错配时抛出
     */
    public SdiOnlineAlterDescriptorPage read(MiniTransaction mtr, PageId pageId) {
        if (mtr == null || pageId == null) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor read requires MTR/page id");
        }
        PageGuard page = mtr.getPage(pool, pageId, PageLatchMode.SHARED);
        FilePageHeader header = PageEnvelope.readHeader(page);
        if (!header.spaceId().equals(pageId.spaceId())
                || header.pageNo() != pageId.pageNo().value()
                || header.pageType() != PageType.DDL_DESCRIPTOR) {
            throw new SdiPageCorruptionException(
                    "online ALTER descriptor envelope mismatch: " + pageId);
        }
        int capacity = PageEnvelopeLayout.trailerOffset(pageSize)
                - PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
        SdiOnlineAlterDescriptorPage decoded;
        try {
            decoded = codec.decode(page.readBytes(
                    PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES, capacity), pageId.spaceId());
        } catch (DatabaseValidationException invalid) {
            throw new SdiPageCorruptionException(
                    "online ALTER descriptor body is invalid: " + pageId, invalid);
        }
        long envelopeNext = header.nextPageNo() == FilePageHeader.FIL_NULL
                ? 0L : header.nextPageNo();
        if (decoded.nextPageNo() != envelopeNext) {
            throw new SdiPageCorruptionException(
                    "online ALTER descriptor body/FIL next mismatch: " + pageId);
        }
        return decoded;
    }

    /**
     * exact-CAS替换既有descriptor body；只允许root level等调用方已经验证的字段变化，owner/chain由模型构造器保持。
     *
     * @param mtr 与最终B+Tree状态刷新协作的写MTR
     * @param pageId chain中的精确页
     * @param expected 更新前完整页视图
     * @param replacement owner/ordinal/chain不变的新视图
     * @throws SdiPageCorruptionException 当前页不等于expected或envelope损坏时抛出
     */
    public void replace(MiniTransaction mtr, PageId pageId,
                        SdiOnlineAlterDescriptorPage expected,
                        SdiOnlineAlterDescriptorPage replacement) {
        if (mtr == null || pageId == null || expected == null || replacement == null
                || expected.ddlOperationId() != replacement.ddlOperationId()
                || expected.targetDictionaryVersion() != replacement.targetDictionaryVersion()
                || expected.tableId() != replacement.tableId()
                || expected.generation() != replacement.generation()
                || !expected.descriptorSegment().equals(replacement.descriptorSegment())
                || expected.pageOrdinal() != replacement.pageOrdinal()
                || expected.nextPageNo() != replacement.nextPageNo()) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor replacement changes immutable owner/chain");
        }
        PageGuard page = mtr.getPage(pool, pageId, PageLatchMode.EXCLUSIVE);
        FilePageHeader header = PageEnvelope.readHeader(page);
        if (!header.spaceId().equals(pageId.spaceId())
                || header.pageNo() != pageId.pageNo().value()
                || header.pageType() != PageType.DDL_DESCRIPTOR) {
            throw new SdiPageCorruptionException(
                    "online ALTER descriptor replacement envelope mismatch: " + pageId);
        }
        int capacity = PageEnvelopeLayout.trailerOffset(pageSize)
                - PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
        byte[] current = page.readBytes(PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES, capacity);
        byte[] expectedBytes = codec.encode(expected);
        if (!java.util.Arrays.equals(current, expectedBytes)) {
            throw new SdiPageCorruptionException(
                    "online ALTER descriptor page changed before replacement: " + pageId);
        }
        page.writeBytes(PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES, codec.encode(replacement));
    }
}
