package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * 页信封访问器（访问器层，import PageGuard）：在 PageGuard 上读写 FilePageHeader 字段，并提供 pageLSN 单字段盖戳/读取。
 *
 * <p>{@link #stampPageLsn} 只写 header PAGE_LSN（恢复幂等用），**不同步 trailer LSN、不重算 checksum**——
 * 那是 flush/checksum 切片（F1）的职责（见 {@link PageChecksum#stamp}）。
 */
public final class PageEnvelope {

    private PageEnvelope() {
    }

    /** 写信封头字段（要求 X）；不写 checksum（由 PageChecksum.stamp 盖）。 */
    public static void writeHeader(PageGuard guard, FilePageHeader h) {
        requireArgs(guard, h);
        guard.writeInt(PageEnvelopeLayout.SPACE_ID, h.spaceId().value());
        guard.writeInt(PageEnvelopeLayout.PAGE_NO, (int) h.pageNo());
        guard.writeInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) h.prevPageNo());
        guard.writeInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) h.nextPageNo());
        guard.writeLong(PageEnvelopeLayout.PAGE_LSN, h.pageLsn());
        guard.writeInt(PageEnvelopeLayout.PAGE_TYPE, h.pageType().code());
    }

    /**
     * 只改写 FIL_PAGE_PREV/NEXT 两个 sibling 字段（要求 X）。B+Tree split 调整 leaf 链时不能复用
     * {@link #writeHeader}，否则会顺带覆盖 pageType/pageLSN 等字段，破坏 WAL 与页类型不变量。
     */
    public static void writeSiblingLinks(PageGuard guard, long prevPageNo, long nextPageNo) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        validateFilPageNo(prevPageNo, "prev");
        validateFilPageNo(nextPageNo, "next");
        guard.writeInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) prevPageNo);
        guard.writeInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) nextPageNo);
    }

    /** 读信封头字段。prev/next 用 &0xFFFFFFFFL 还原（-1→FIL_NULL）。 */
    public static FilePageHeader readHeader(PageGuard guard) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        return new FilePageHeader(
                SpaceId.of(guard.readInt(PageEnvelopeLayout.SPACE_ID)),
                guard.readInt(PageEnvelopeLayout.PAGE_NO) & 0xFFFFFFFFL,
                guard.readInt(PageEnvelopeLayout.PREV_PAGE_NO) & 0xFFFFFFFFL,
                guard.readInt(PageEnvelopeLayout.NEXT_PAGE_NO) & 0xFFFFFFFFL,
                guard.readLong(PageEnvelopeLayout.PAGE_LSN),
                PageType.fromCode(guard.readInt(PageEnvelopeLayout.PAGE_TYPE)));
    }

    /** 仅盖 header pageLSN（要求 X）。MTR commit 用：分配 endLsn 后给 touched 页盖戳。 */
    public static void stampPageLsn(PageGuard guard, Lsn lsn) {
        if (guard == null || lsn == null) {
            throw new DatabaseValidationException("page guard / lsn must not be null");
        }
        guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
    }

    /** 读 header pageLSN。 */
    public static Lsn readPageLsn(PageGuard guard) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        return Lsn.of(guard.readLong(PageEnvelopeLayout.PAGE_LSN));
    }

    private static void requireArgs(PageGuard guard, FilePageHeader h) {
        if (guard == null || h == null) {
            throw new DatabaseValidationException("page guard / header must not be null");
        }
    }

    private static void validateFilPageNo(long pageNo, String name) {
        if (pageNo < 0 || pageNo > FilePageHeader.FIL_NULL) {
            throw new DatabaseValidationException(name + " page no out of FIL u32 range: " + pageNo);
        }
    }
}
