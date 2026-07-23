package cn.zhangyis.db.storage.changebuffer;

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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每个 tablespace 的 4-bit/page Change Buffer bitmap 仓储。它只维护空间等级、buffered 与 internal 三个字段，
 * 不解析目标 INDEX 页；同 byte 两个 nibble 的读改写由 bitmap 页 X latch 原子保护。
 */
public final class ChangeBufferBitmapRepository {

    /** bitmap 页 fix、latch 与脏页发布入口。 */
    private final BufferPool pool;
    /** 重复 bitmap 寻址和 trailer 边界使用的实例页大小。 */
    private final PageSize pageSize;
    /** 当前进程已通过 FIL identity/type 校验的不同 bitmap 页，仅供诊断计数，不替代持久 bitmap。 */
    private final Set<PageId> observedBitmapPages = ConcurrentHashMap.newKeySet();

    /**
     * @param pool 共享 Buffer Pool；不得为 {@code null}
     * @param pageSize 实例固定页大小；不得为 {@code null}
     */
    public ChangeBufferBitmapRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer bitmap repository dependencies must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /**
     * 判断一个物理 identity 是否可由用户空间重复 bitmap 维护。系统空间、首区固定页和任意重复 bitmap
     * 自身返回 {@code false}；普通已分配页所在管理区已由 FSP 在 freeLimit 越界前格式化对应 bitmap。
     *
     * @param targetPageId 候选二级 leaf identity；不得为 {@code null}
     * @return 非系统空间 page 3 及以后且不是 bitmap 固定页时为 {@code true}
     * @throws DatabaseValidationException 参数为空时抛出
     */
    public boolean supportsTarget(PageId targetPageId) {
        if (targetPageId == null) {
            throw new DatabaseValidationException("change buffer bitmap target page must not be null");
        }
        return !targetPageId.spaceId().equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                && targetPageId.pageNo().value() >= 3L
                && !ChangeBufferBitmapLayout.isBitmapPage(pageSize, targetPageId.pageNo());
    }

    /**
     * 读取目标页对应 nibble。重复 bitmap 页必须已经由 FSP 格式化；缺失或页类型错误均 fail-closed。
     *
     * @param mtr 活动 MTR；不得为 {@code null}
     * @param targetPageId 目标二级 leaf 的物理 identity；不得为 {@code null}
     * @return 当前保守空闲等级、buffered 和 internal 标志
     * @throws ChangeBufferFormatException bitmap envelope 损坏或相对偏移越界时抛出
     */
    public ChangeBufferBitmapState read(MiniTransaction mtr, PageId targetPageId) {
        return read(mtr, targetPageId, PageLatchMode.SHARED);
    }

    /**
     * 直接以 X latch 读取目标 nibble，供同一 MTR 随后保留 buffered/internal 位并重算空闲等级；
     * 禁止普通调用方先 {@link #read} 再 {@link #write} 形成 S→X 升级。
     *
     * @param mtr 活动写 MTR；不得为 {@code null}
     * @param targetPageId 目标二级 leaf identity；不得为 {@code null}
     * @return X latch 下读取的完整四位状态，guard 继续归当前 MTR
     */
    public ChangeBufferBitmapState readForUpdate(MiniTransaction mtr, PageId targetPageId) {
        return read(mtr, targetPageId, PageLatchMode.EXCLUSIVE);
    }

    private ChangeBufferBitmapState read(MiniTransaction mtr, PageId targetPageId, PageLatchMode mode) {
        requireArguments(mtr, targetPageId);
        requireSupportedTarget(targetPageId);
        PageId bitmapPageId = bitmapPageId(targetPageId);
        PageGuard guard = mtr.getPage(pool, bitmapPageId, mode);
        validateEnvelope(guard, bitmapPageId);
        observedBitmapPages.add(bitmapPageId);
        int byteOffset = byteOffset(targetPageId);
        byte packed = guard.readBytes(byteOffset, 1)[0];
        return ChangeBufferBitmapLayout.decodeNibble(
                packed, ChangeBufferBitmapLayout.nibbleShift(pageSize, targetPageId.pageNo()));
    }

    /**
     * 原子改写一个目标 nibble，同时保留同 byte 中相邻页的四位状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>根据目标 SpaceId/PageNo 计算唯一 bitmap page、byte 与 nibble，不读取目标数据页。</li>
     *     <li>以 X latch 固定 bitmap 页并复核 FIL identity/type，损坏时不覆盖证据。</li>
     *     <li>读取原 byte、只替换目标四位并写回；PAGE_BYTES redo 与脏页状态归当前 MTR。</li>
     *     <li>返回写入状态；提交前调用方不得据此对其它线程宣称 mutation 已持久化。</li>
     * </ol>
     *
     * @param mtr 活动写 MTR；不得为 {@code null}
     * @param targetPageId 目标二级 leaf；不得为 {@code null}
     * @param state 待写入的完整四位逻辑状态；不得为 {@code null}
     * @return 原样返回的已校验状态
     * @throws ChangeBufferFormatException bitmap envelope 损坏时抛出
     */
    public ChangeBufferBitmapState write(MiniTransaction mtr, PageId targetPageId,
                                         ChangeBufferBitmapState state) {
        // 1、纯计算定位，非法 identity 在任何 fix 前拒绝。
        requireArguments(mtr, targetPageId);
        requireSupportedTarget(targetPageId);
        if (state == null) {
            throw new DatabaseValidationException("change buffer bitmap state must not be null");
        }
        PageId bitmapPageId = bitmapPageId(targetPageId);
        int byteOffset = byteOffset(targetPageId);
        int shift = ChangeBufferBitmapLayout.nibbleShift(pageSize, targetPageId.pageNo());
        // 2、X latch 保护同 byte 的读改写，并先验证专用页类型。
        PageGuard guard = mtr.getPage(pool, bitmapPageId, PageLatchMode.EXCLUSIVE);
        validateEnvelope(guard, bitmapPageId);
        observedBitmapPages.add(bitmapPageId);
        // 3、掩码只替换目标 nibble，不能清除相邻页的状态。
        int packed = Byte.toUnsignedInt(guard.readBytes(byteOffset, 1)[0]);
        int mask = 0xF << shift;
        int nibble = ChangeBufferBitmapLayout.encodeNibble(
                state.freeSpaceClass(), state.buffered(), state.changeBufferInternal());
        guard.writeBytes(byteOffset, new byte[]{(byte) ((packed & ~mask) | (nibble << shift))});
        // 4、持久发布由 MTR commit 决定，本方法只返回本次写入的领域值。
        return state;
    }

    /**
     * 清空一个已格式化的重复 bitmap 页 body。IMPORT 不能相信外部文件携带的 free-space、buffered 或 internal
     * 位；本方法保留 FIL envelope，只把该覆盖区间的全部 4-bit entry 归零，并由当前 MTR 记录页面 redo。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验目标属于非系统空间且页号满足重复 bitmap 公式，拒绝把普通数据页当作 bitmap 覆盖。</li>
     *     <li>以 X latch 固定页面并复核 FIL identity/type；导入格式不兼容时在首个字节写之前 fail-closed。</li>
     *     <li>清零 FIL header 与 trailer 之间的完整 body，使同一覆盖区间所有 nibble 同时失效。</li>
     *     <li>登记诊断观察并返回；redo durable 与数据文件 force 仍由 IMPORT 上层按 WAL 顺序完成。</li>
     * </ol>
     *
     * @param mtr IMPORT 独占表空间 lease 内创建的活动写 MTR
     * @param bitmapPageId 待重建的非系统 IBUF_BITMAP 物理页
     * @throws DatabaseValidationException 参数为空、系统空间或页号不满足 bitmap 公式时抛出
     * @throws ChangeBufferFormatException 页面 envelope identity/type 损坏或不是受支持格式时抛出
     */
    public void resetBitmapPage(MiniTransaction mtr, PageId bitmapPageId) {
        // 1、只允许公式明确保留的用户 bitmap identity，避免 IMPORT 静默覆盖普通记录页。
        requireArguments(mtr, bitmapPageId);
        if (bitmapPageId.spaceId().equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                || !ChangeBufferBitmapLayout.isBitmapPage(pageSize, bitmapPageId.pageNo())) {
            throw new DatabaseValidationException(
                    "change buffer bitmap reset requires a non-system bitmap page: " + bitmapPageId);
        }
        // 2、页面类型是外部文件兼容性的硬边界；不匹配时必须保留原文件供 DDL recovery/诊断。
        PageGuard guard = mtr.getPage(pool, bitmapPageId, PageLatchMode.EXCLUSIVE);
        validateEnvelope(guard, bitmapPageId);

        // 3、body 不含其它 FSP 元数据；整段归零同时撤销旧 incarnation 的三类 bitmap 状态。
        int bodyOffset = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
        int bodyLength = PageEnvelopeLayout.trailerOffset(pageSize) - bodyOffset;
        guard.writeBytes(bodyOffset, new byte[bodyLength]);

        // 4、观测集合不参与正确性；MTR commit 才是 reset 已发布的边界。
        observedBitmapPages.add(bitmapPageId);
    }

    /**
     * @return 相邻重复 bitmap 页的页号间隔；等于实例物理页字节数，供 IMPORT 有界枚举当前物理文件
     */
    public int bitmapPageInterval() {
        return pageSize.bytes();
    }

    /**
     * @return 当前进程至少成功读取或改写过一次的不同 bitmap 物理页数；弱一致，仅用于控制面诊断
     */
    public long observedBitmapPageCount() {
        return observedBitmapPages.size();
    }

    private PageId bitmapPageId(PageId targetPageId) {
        return PageId.of(targetPageId.spaceId(),
                ChangeBufferBitmapLayout.bitmapPageNo(pageSize, targetPageId.pageNo()));
    }

    private int byteOffset(PageId targetPageId) {
        int offset = Math.addExact(PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES,
                ChangeBufferBitmapLayout.byteOffsetInBody(pageSize, targetPageId.pageNo()));
        if (offset >= PageEnvelopeLayout.trailerOffset(pageSize)) {
            throw new ChangeBufferFormatException("change buffer bitmap byte overlaps FIL trailer: " + offset);
        }
        return offset;
    }

    private static void validateEnvelope(PageGuard guard, PageId expected) {
        FilePageHeader header;
        try {
            header = PageEnvelope.readHeader(guard);
        } catch (RuntimeException invalid) {
            throw new ChangeBufferFormatException("change buffer bitmap FIL envelope is invalid", invalid);
        }
        if (!header.spaceId().equals(expected.spaceId()) || header.pageNo() != expected.pageNo().value()
                || header.pageType() != PageType.IBUF_BITMAP) {
            throw new ChangeBufferFormatException("change buffer bitmap page identity/type mismatch: " + header);
        }
    }

    private static void requireArguments(MiniTransaction mtr, PageId targetPageId) {
        if (mtr == null || targetPageId == null) {
            throw new DatabaseValidationException("change buffer bitmap MTR/target page must not be null");
        }
    }

    private void requireSupportedTarget(PageId targetPageId) {
        if (!supportsTarget(targetPageId)) {
            throw new DatabaseValidationException(
                    "change buffer bitmap target is outside the managed user range: " + targetPageId);
        }
    }
}
