package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
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
 * system.ibd 固定 page 3 的持久仓储。仓储只解释 Change Buffer header，不理解 B+Tree record；
 * 所有读写都由调用方 MTR 持有 page latch、收集 redo 并在提交时盖 page LSN。
 */
public final class ChangeBufferHeaderRepository {

    /** Change Buffer header 在系统表空间中的固定物理位置。 */
    public static final PageId HEADER_PAGE_ID = PageId.of(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID, PageNo.of(3));

    /** 页面 fix/latch 的唯一入口；生命周期由组合根持有。 */
    private final BufferPool pool;
    /** 用于证明 header body 不覆盖 FIL trailer 的实例固定页大小。 */
    private final PageSize pageSize;

    /**
     * @param pool 共享 Buffer Pool；不得为 {@code null}
     * @param pageSize 与 system.ibd page0 声明一致的实例页大小；不得为 {@code null}
     */
    public ChangeBufferHeaderRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer header repository dependencies must not be null");
        }
        if (bodyOffset() + ChangeBufferHeaderCodec.ENCODED_LENGTH
                > PageEnvelopeLayout.trailerOffset(pageSize)) {
            throw new DatabaseValidationException("change buffer header does not fit page body");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /**
     * 首次格式化固定 page 3，并在同一 MTR 写入自校验 header。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验活动 MTR 与完整快照，任何错误早于 newPage 和 redo 副作用。</li>
     *     <li>以 IBUF_HEADER 类型创建固定页并取得 X latch，页面由 MTR memo 持有。</li>
     *     <li>写 FIL envelope 与带 CRC 的 v1 body，PAGE_INIT/PAGE_BYTES 进入同一 redo 批次。</li>
     *     <li>返回原快照；只有调用方提交 MTR 后该 header 才可发布给普通 Change Buffer 流量。</li>
     * </ol>
     *
     * @param mtr 引擎 bootstrap 的活动写 MTR；不得为 {@code null}
     * @param snapshot 已分配 root/segment 后构造的初始权威快照；不得为 {@code null}
     * @return 与写入字节完全对应的初始快照
     * @throws DatabaseValidationException 参数为空或 body 无法容纳时抛出
     */
    public ChangeBufferHeaderSnapshot format(MiniTransaction mtr, ChangeBufferHeaderSnapshot snapshot) {
        // 1、先验证对象边界，避免无效 header 把固定系统页重新初始化。
        requireArguments(mtr, snapshot);
        // 2、固定 page 3 并声明专用持久页类型；guard 生命周期归 MTR。
        PageGuard guard = mtr.newPage(pool, HEADER_PAGE_ID, PageLatchMode.EXCLUSIVE, PageType.IBUF_HEADER);
        // 3、先建立可独立诊断的 FIL identity，再写带内部 CRC 的 header body。
        PageEnvelope.writeHeader(guard, new FilePageHeader(HEADER_PAGE_ID.spaceId(),
                HEADER_PAGE_ID.pageNo().value(), FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL,
                0L, PageType.IBUF_HEADER));
        guard.writeBytes(bodyOffset(), ChangeBufferHeaderCodec.encode(snapshot));
        // 4、快照不从可变页视图逃逸，提交与发布仍由上层组合根负责。
        return snapshot;
    }

    /**
     * 以共享 latch 读取 page 3；返回值是独立不可变快照，方法不提前释放 MTR 资源。
     *
     * @param mtr 活动只读或写 MTR；不得为 {@code null}
     * @return 通过 envelope、magic、版本与 CRC 校验的权威快照
     * @throws ChangeBufferFormatException page identity/type 或 body 损坏时抛出，调用方不得启用缓冲写
     */
    public ChangeBufferHeaderSnapshot read(MiniTransaction mtr) {
        return read(mtr, PageLatchMode.SHARED);
    }

    /**
     * 直接以 X latch 读取 header，供随后在同一 MTR 更新 sequence、pending 与 root level；禁止先 S 后升级。
     *
     * @param mtr 活动写 MTR；不得为 {@code null}
     * @return 当前 page 3 权威快照；X guard 继续由 MTR 持有
     * @throws ChangeBufferFormatException 持久页不能安全解释时抛出
     */
    public ChangeBufferHeaderSnapshot readForUpdate(MiniTransaction mtr) {
        return read(mtr, PageLatchMode.EXCLUSIVE);
    }

    /**
     * 在调用方已规划好的 MTR 中覆盖 page 3 body；通常该 MTR 已经通过 {@link #readForUpdate} 持有 X latch，
     * 重入 fix 不会形成 S→X 升级，提交失败时不会发布新的 header 计数。
     *
     * @param mtr 活动写 MTR；不得为 {@code null}
     * @param snapshot 与全局树本次结构变化一致的新快照；不得为 {@code null}
     * @throws ChangeBufferFormatException 固定页 identity/type 已损坏时抛出
     */
    public void write(MiniTransaction mtr, ChangeBufferHeaderSnapshot snapshot) {
        requireArguments(mtr, snapshot);
        PageGuard guard = mtr.getPage(pool, HEADER_PAGE_ID, PageLatchMode.EXCLUSIVE);
        validateEnvelope(guard);
        guard.writeBytes(bodyOffset(), ChangeBufferHeaderCodec.encode(snapshot));
    }

    private ChangeBufferHeaderSnapshot read(MiniTransaction mtr, PageLatchMode mode) {
        if (mtr == null) {
            throw new DatabaseValidationException("change buffer header MTR must not be null");
        }
        PageGuard guard = mtr.getPage(pool, HEADER_PAGE_ID, mode);
        validateEnvelope(guard);
        return ChangeBufferHeaderCodec.decode(guard.readBytes(bodyOffset(), ChangeBufferHeaderCodec.ENCODED_LENGTH));
    }

    private void validateEnvelope(PageGuard guard) {
        FilePageHeader header;
        try {
            header = PageEnvelope.readHeader(guard);
        } catch (RuntimeException invalid) {
            throw new ChangeBufferFormatException("change buffer header FIL envelope is invalid", invalid);
        }
        if (!header.spaceId().equals(HEADER_PAGE_ID.spaceId())
                || header.pageNo() != HEADER_PAGE_ID.pageNo().value()
                || header.pageType() != PageType.IBUF_HEADER) {
            throw new ChangeBufferFormatException("change buffer header page identity/type mismatch: " + header);
        }
    }

    private static int bodyOffset() {
        return PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    }

    private static void requireArguments(MiniTransaction mtr, ChangeBufferHeaderSnapshot snapshot) {
        if (mtr == null || snapshot == null) {
            throw new DatabaseValidationException("change buffer header MTR/snapshot must not be null");
        }
    }
}
