package cn.zhangyis.db.storage.api.index;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.record.page.RecordPage;

/**
 * MTR-owned INDEX 页句柄。它把同一个 {@link PageGuard} 暴露成 file header 访问和 record-page 访问两个视图，
 * 但不拥有释放职责；guard 仍由 {@code MiniTransaction} memo 在 commit/rollback 时统一释放。
 *
 * <p>B+Tree split 需要同时读写 FIL sibling 链和 INDEX record 区，使用本句柄可以避免把底层 BufferFrame 暴露给 btree 包。
 */
public final class IndexPageHandle {

    /** 页物理定位；用于产出 RecordRef 或写 sibling 链诊断，不代表 segment 所有权。 */
    private final PageId pageId;
    /** MTR memo 持有的 page guard；本类只借用，不 close。 */
    private final PageGuard guard;
    /** 页大小；RecordPage 需要它定位 page directory 与 trailer。 */
    private final PageSize pageSize;

    /**
     * 创建 {@code IndexPageHandle}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public IndexPageHandle(PageId pageId, PageGuard guard, PageSize pageSize) {
        if (pageId == null || guard == null || pageSize == null) {
            throw new DatabaseValidationException("index page handle pageId/guard/pageSize must not be null");
        }
        this.pageId = pageId;
        this.guard = guard;
        this.pageSize = pageSize;
    }

    /** 返回该句柄绑定的物理页号。 */
    public PageId pageId() {
        return pageId;
    }

    /**
     * 包内可见的 guard 访问：仅供同包 {@link IndexPageAccess#releaseHandle} 在 B+Tree 写路径 latch coupling 时
     * 把 guard 交回 MTR 做选择性提前释放。刻意不 public，保持 btree 等上层只经句柄操作、绝不接触裸 guard/frame。
     */
    PageGuard guard() {
        return guard;
    }

    /** 读取 FIL file page header；不解析 record 区。 */
    public FilePageHeader fileHeader() {
        return PageEnvelope.readHeader(guard);
    }

    /** 返回绑定同一 guard 的 RecordPage 视图；调用方必须在当前 MTR 生命周期内使用。 */
    public RecordPage recordPage() {
        return new RecordPage(guard, pageSize);
    }

    /**
     * 窄写 leaf sibling 链，只改 FIL_PAGE_PREV/NEXT。该方法要求当前 guard 为 EXCLUSIVE，
     * PageGuard 写原语会在模式不匹配时抛领域校验异常。
     *
     * @param prevPageNo 参与 {@code writeSiblingLinks} 的原始数值身份 {@code prevPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param nextPageNo 参与 {@code writeSiblingLinks} 的原始数值身份 {@code nextPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    public void writeSiblingLinks(long prevPageNo, long nextPageNo) {
        PageEnvelope.writeSiblingLinks(guard, prevPageNo, nextPageNo);
    }
}
