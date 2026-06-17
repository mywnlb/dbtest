package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.buf.PageGuard;

import java.util.Objects;

/**
 * 页内地址（InnoDB fil_addr_t 最小版）：PageNo + 页内 offset，或 NULL 哨兵。只存取、不走链（链表算法属 fsp 分配切片）。
 *
 * <p>编码 12 字节：pageNo(long 8) + offset(int 4)。NULL = 全零编码（pageNoRaw==0 且 offset==0），
 * 使刚 create 的全零页的 list 指针天然解码为 NULL；真实节点偏移恒 ≥ FIL_PAGE_DATA，绝不落在 (page0,offset0)，无碰撞。
 */
public final class FileAddress {

    /** NULL 哨兵：全零编码，表示空 free-list 头或链尾。 */
    public static final FileAddress NULL = new FileAddress(null, 0, true);

    private final PageNo pageNo;
    private final int offset;
    private final boolean nil;

    private FileAddress(PageNo pageNo, int offset, boolean nil) {
        this.pageNo = pageNo;
        this.offset = offset;
        this.nil = nil;
    }

    /**
     * 创建非空页内地址。
     *
     * @param pageNo 所在页。
     * @param offset 页内偏移（≥0）；(page0,offset0) 保留作 NULL，拒绝。
     * @return 页内地址。
     */
    public static FileAddress of(PageNo pageNo, int offset) {
        Objects.requireNonNull(pageNo, "pageNo");
        if (offset < 0) {
            throw new DatabaseValidationException("file address offset must be non-negative: " + offset);
        }
        if (pageNo.value() == 0 && offset == 0) {
            throw new DatabaseValidationException("(page0, offset0) is reserved as NULL; use FileAddress.NULL");
        }
        return new FileAddress(pageNo, offset, false);
    }

    /** 是否为 NULL 哨兵。 */
    public boolean isNull() {
        return nil;
    }

    /** 所在页；NULL 调用抛异常。 */
    public PageNo pageNo() {
        if (nil) {
            throw new DatabaseValidationException("NULL file address has no pageNo");
        }
        return pageNo;
    }

    /** 页内偏移；NULL 调用抛异常。 */
    public int offset() {
        if (nil) {
            throw new DatabaseValidationException("NULL file address has no offset");
        }
        return offset;
    }

    /** 编码 12 字节写入 guard（要求 X latch）：pageNoRaw(long)+offset(int)。NULL→全零。 */
    public void writeTo(PageGuard guard, int at) {
        if (nil) {
            guard.writeLong(at, 0L);
            guard.writeInt(at + Long.BYTES, 0);
        } else {
            guard.writeLong(at, pageNo.value());
            guard.writeInt(at + Long.BYTES, offset);
        }
    }

    /** 从 guard 解码 12 字节。全零→NULL。 */
    public static FileAddress readFrom(PageGuard guard, int at) {
        long pageNoRaw = guard.readLong(at);
        int off = guard.readInt(at + Long.BYTES);
        if (pageNoRaw == 0 && off == 0) {
            return NULL;
        }
        return of(PageNo.of(pageNoRaw), off);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FileAddress that)) {
            return false;
        }
        if (nil || that.nil) {
            return nil == that.nil;
        }
        return offset == that.offset && pageNo.equals(that.pageNo);
    }

    @Override
    public int hashCode() {
        return nil ? 0 : Objects.hash(pageNo, offset);
    }

    @Override
    public String toString() {
        return nil ? "FileAddress.NULL" : "FileAddress(" + pageNo.value() + "," + offset + ")";
    }
}
