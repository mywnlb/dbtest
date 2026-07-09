package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * XDES（extent descriptor）仓储（设计 §6.3）。首版 XDES entries 内嵌 page 0；按 ExtentId.extentNo 定位 slot。
 * 物理空间账本：extent 状态 / 归属 segment / list-node 指针 / page 分配位图。读写经 page 0 latch（写 X）。
 * extent 0 是系统保留 extent，不能走普通 initFree/free-list 分配路径，也不能被普通 XDES mutator 改写。
 * extentNo 超 page 0 首批容量 → FspMetadataException（首版不支持独立 XDES 管理页）。
 *
 * <p>简化点：1 位/页 bitmap（不分 used/clean 两位）；本切片 no-redo，写页只标脏、不产 redo（设计 §15 redo 规则推迟满足），
 * 未来 redo 切片接入 {@code UPDATE_XDES} 后才能 crash-safe。
 */
public final class ExtentDescriptorRepository {

    /** 受控页来源；XDES entries 内嵌 page 0，经 MTR.getPage 拿 page 0 的 PageGuard。 */
    private final BufferPool pool;

    /** 实例级页大小；决定每 extent 有效页数（pagesPerExtent）与 page 0 首批 XDES 容量。 */
    private final PageSize pageSize;

    public ExtentDescriptorRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    private int entryOffset(ExtentId extentId) {
        requireExtent(extentId);
        if (extentId.extentNo() >= ExtentDescriptorLayout.maxEntriesInPage0(pageSize)) {
            throw new FspMetadataException("extent beyond first XDES region not supported: " + extentId.extentNo());
        }
        return ExtentDescriptorLayout.entryOffset(extentId.extentNo());
    }

    private PageId page0(ExtentId extentId) {
        return PageId.of(extentId.spaceId(), PageNo.of(0));
    }

    public ExtentDescriptor read(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        ExtentState state = decodeState(g.readInt(base + ExtentDescriptorLayout.STATE));
        long owner = g.readLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
        FileAddress prev = FileAddress.readFrom(g, base + ExtentDescriptorLayout.PREV);
        FileAddress next = FileAddress.readFrom(g, base + ExtentDescriptorLayout.NEXT);
        return new ExtentDescriptor(extentId, state, owner, prev, next);
    }

    /** 该 extent 的 FLST 链节点起址 = page0 内 entry 的 prev 字段偏移；供 Flst/分配层把 extent 入链。 */
    public FileAddress listNodeAddr(ExtentId extentId) {
        int base = entryOffset(extentId);
        return FileAddress.of(PageNo.of(0), base + ExtentDescriptorLayout.PREV);
    }

    /** 反向：由链节点地址还原 ExtentId。节点必在 page0、偏移须按 ENTRY_SIZE 对齐，否则视为页上链指针损坏。 */
    public ExtentId extentIdOfNode(SpaceId spaceId, FileAddress nodeAddr) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (nodeAddr == null || nodeAddr.isNull()) {
            throw new DatabaseValidationException("node address must be concrete");
        }
        if (nodeAddr.pageNo().value() != 0) {
            throw new FspMetadataException("xdes list node must be on page 0: " + nodeAddr);
        }
        int rel = nodeAddr.offset() - SpaceHeaderLayout.XDES_BASE - ExtentDescriptorLayout.PREV;
        if (rel < 0 || rel % ExtentDescriptorLayout.ENTRY_SIZE != 0) {
            throw new FspMetadataException("misaligned xdes list node offset: " + nodeAddr.offset());
        }
        return ExtentId.of(spaceId, rel / ExtentDescriptorLayout.ENTRY_SIZE);
    }

    /** extent 内首个未分配页下标（S）；满则 empty。仅扫前 pagesPerExtent 位。 */
    public OptionalInt firstFreePageIndex(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        int pe = pageSize.pagesPerExtent();
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP, ExtentDescriptorLayout.BITMAP_BYTES);
        for (int i = 0; i < pe; i++) {
            if ((bm[i / 8] & (1 << (i % 8))) == 0) {
                return OptionalInt.of(i);
            }
        }
        return OptionalInt.empty();
    }

    /** extent 内已分配页数（S），仅统计前 pagesPerExtent 位。 */
    public int allocatedPageCount(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        int pe = pageSize.pagesPerExtent();
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP, ExtentDescriptorLayout.BITMAP_BYTES);
        int count = 0;
        for (int i = 0; i < pe; i++) {
            if ((bm[i / 8] & (1 << (i % 8))) != 0) {
                count++;
            }
        }
        return count;
    }

    /** extent 是否所有页已分配。 */
    public boolean isFull(MiniTransaction mtr, ExtentId extentId) {
        return allocatedPageCount(mtr, extentId) == pageSize.pagesPerExtent();
    }

    /** extent 是否全空。 */
    public boolean isEmpty(MiniTransaction mtr, ExtentId extentId) {
        return allocatedPageCount(mtr, extentId) == 0;
    }

    /** 重置普通 extent 为零态（FREE/无主/NULL/bitmap 清零）。extent0 系统保留，禁止普通初始化。 */
    public void initFree(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        if (extentId != null && extentId.extentNo() == 0) {
            throw new FspMetadataException("extent 0 is system-reserved; use reserveSystemExtent");
        }
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, ExtentState.FREE, "initialize FREE extent state");
        writeOwnerImage(mtr, g, extentId, base, 0L, "initialize FREE extent owner");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "initialize FREE extent prev");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "initialize FREE extent next");
        FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.BITMAP_BYTES], "initialize FREE extent bitmap");
    }

    /** 初始化/修复 extent0 系统保留状态：page0..3 固定管理页标记已分配，避免普通 allocator 误用。 */
    public void reserveSystemExtent(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        ExtentId extentId = ExtentId.of(spaceId, 0);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, ExtentState.FSEG_FRAG, "reserve system extent state");
        writeOwnerImage(mtr, g, extentId, base, 0L, "reserve system extent owner");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "reserve system extent prev");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "reserve system extent next");
        FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.BITMAP_BYTES], "reserve system extent bitmap");
        for (int page = 0; page < 4; page++) {
            int byteOffset = base + ExtentDescriptorLayout.BITMAP + page / 8;
            byte b = g.readBytes(byteOffset, 1)[0];
            FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                    extentId.extentNo(), page / 8, byteOffset,
                    new byte[] {(byte) (b | (1 << (page % 8)))}, "reserve system extent allocated bit");
        }
    }

    public void writeState(MiniTransaction mtr, ExtentId extentId, ExtentState state) {
        requireMtr(mtr);
        if (state == null) {
            throw new DatabaseValidationException("extent state must not be null");
        }
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, state, "write XDES state");
    }

    public void writeOwner(MiniTransaction mtr, ExtentId extentId, Optional<SegmentId> owner) {
        requireMtr(mtr);
        if (owner == null) {
            throw new DatabaseValidationException("owner optional must not be null");
        }
        requireOrdinaryMutableExtent(extentId);
        long raw = owner.map(SegmentId::value).orElse(0L);
        if (raw == 0 && owner.isPresent()) {
            throw new DatabaseValidationException("segment id 0 is reserved as XDES owner sentinel");
        }
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeOwnerImage(mtr, g, extentId, base, raw, "write XDES owner");
    }

    public void writePrev(MiniTransaction mtr, ExtentId extentId, FileAddress prev) {
        writeAddr(mtr, extentId, ExtentDescriptorLayout.PREV, prev);
    }

    public void writeNext(MiniTransaction mtr, ExtentId extentId, FileAddress next) {
        writeAddr(mtr, extentId, ExtentDescriptorLayout.NEXT, next);
    }

    public boolean isPageAllocated(MiniTransaction mtr, ExtentId extentId, int pageIndexInExtent) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        requireBitIndex(pageIndexInExtent);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte b = g.readBytes(base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8, 1)[0];
        return (b & (1 << (pageIndexInExtent % 8))) != 0;
    }

    public void setPageAllocated(MiniTransaction mtr, ExtentId extentId, int pageIndexInExtent, boolean allocated) {
        requireMtr(mtr);
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        requireBitIndex(pageIndexInExtent);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        int byteOffset = base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8;
        int mask = 1 << (pageIndexInExtent % 8);
        byte b = g.readBytes(byteOffset, 1)[0];
        byte nb = (byte) (allocated ? (b | mask) : (b & ~mask));
        FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), pageIndexInExtent / 8, byteOffset, new byte[] {nb},
                "set XDES page allocation bit");
    }

    private void writeAddr(MiniTransaction mtr, ExtentId extentId, int fieldOffset, FileAddress addr) {
        requireMtr(mtr);
        if (addr == null) {
            throw new DatabaseValidationException("file address must not be null (use FileAddress.NULL)");
        }
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeAddressImage(mtr, g, extentId, base + fieldOffset, addr, fieldOffset, "write XDES list address");
    }

    private void writeStateImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int base,
                                 ExtentState state, String reason) {
        FspRedoDeltas.writeInt(mtr, guard, page0(extentId), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), ExtentDescriptorLayout.STATE,
                base + ExtentDescriptorLayout.STATE, state.ordinal(), reason);
    }

    private void writeOwnerImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int base,
                                 long owner, String reason) {
        FspRedoDeltas.writeLong(mtr, guard, page0(extentId), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), ExtentDescriptorLayout.OWNER_SEGMENT,
                base + ExtentDescriptorLayout.OWNER_SEGMENT, owner, reason);
    }

    private void writeAddressImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int offset,
                                   FileAddress address, int fieldOffset, String reason) {
        FspRedoDeltas.writeAddress(mtr, guard, page0(extentId), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), fieldOffset, offset, address, reason);
    }

    private static ExtentState decodeState(int ordinal) {
        ExtentState[] values = ExtentState.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new FspMetadataException("invalid extent state ordinal on disk: " + ordinal);
        }
        return values[ordinal];
    }

    private void requireBitIndex(int idx) {
        if (idx < 0 || idx >= pageSize.pagesPerExtent()) {
            throw new DatabaseValidationException("page index in extent out of range: " + idx);
        }
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireExtent(ExtentId extentId) {
        if (extentId == null) {
            throw new DatabaseValidationException("extent id must not be null");
        }
    }

    private static void requireOrdinaryMutableExtent(ExtentId extentId) {
        requireExtent(extentId);
        if (extentId.extentNo() == 0) {
            throw new FspMetadataException("extent 0 is system-reserved; use reserveSystemExtent");
        }
    }
}
