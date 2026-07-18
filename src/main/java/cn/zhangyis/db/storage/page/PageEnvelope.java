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

    /** 写信封头字段（要求 X）；不写 checksum（由 PageChecksum.stamp 盖）。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param h 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
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
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param prevPageNo 参与 {@code writeSiblingLinks} 的原始数值身份 {@code prevPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code writeSiblingLinks} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /** 读信封头字段。prev/next 用 &0xFFFFFFFFL 还原（-1→FIL_NULL）。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code readHeader} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** 仅盖 header pageLSN（要求 X）。MTR commit 用：分配 endLsn 后给 touched 页盖戳。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param lsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void stampPageLsn(PageGuard guard, Lsn lsn) {
        if (guard == null || lsn == null) {
            throw new DatabaseValidationException("page guard / lsn must not be null");
        }
        guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
    }

    /** 读 header pageLSN。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code readPageLsn} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
