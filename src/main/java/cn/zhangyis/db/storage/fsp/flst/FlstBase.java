package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * FLST 链表 base（表头，InnoDB FLST base = len + first + last）。空链 = {@link #EMPTY} = (0, NULL, NULL)。
 *
 * <p>校验分两层，避免把磁盘损坏当成程序错误：record 构造只校验 length>=0（{@link DatabaseValidationException}）；
 * 磁盘解码（{@link #readFrom}）额外校验空链一致性 length==0 ⇔ first/last 均 NULL，不一致抛 {@link FspMetadataException}。
 *
 * @param length 链长（节点数，非负）。
 * @param first  首节点地址；空链为 NULL。
 * @param last   尾节点地址；空链为 NULL。
 */
public record FlstBase(long length, FileAddress first, FileAddress last) {

    /** 空链常量：(0, NULL, NULL)。零初始化页天然解码为它。 */
    public static final FlstBase EMPTY = new FlstBase(0L, FileAddress.NULL, FileAddress.NULL);

    public FlstBase {
        if (first == null || last == null) {
            throw new DatabaseValidationException("flst base first/last must not be null (use FileAddress.NULL)");
        }
        if (length < 0) {
            throw new DatabaseValidationException("flst base length must be non-negative: " + length);
        }
    }

    /** 编码 32 字节写入 guard（要求 X latch）：len(8)+first(12)+last(12)。 */
    public void writeTo(PageGuard guard, int at) {
        guard.writeLong(at + FlstBaseLayout.LEN, length);
        first.writeTo(guard, at + FlstBaseLayout.FIRST);
        last.writeTo(guard, at + FlstBaseLayout.LAST);
    }

    /**
     * 从 guard 解码 32 字节，并校验空链一致性：length==0 ⇔ first/last 均 NULL；length>0 ⇔ first/last 均非 NULL。
     * 不一致视为页上链账本损坏，抛 {@link FspMetadataException}（与构造期 {@link DatabaseValidationException} 区分）。
     */
    public static FlstBase readFrom(PageGuard guard, int at) {
        long length = guard.readLong(at + FlstBaseLayout.LEN);
        FileAddress first = FileAddress.readFrom(guard, at + FlstBaseLayout.FIRST);
        FileAddress last = FileAddress.readFrom(guard, at + FlstBaseLayout.LAST);
        if (length < 0) {
            throw new FspMetadataException("invalid flst base length on disk: " + length);
        }
        boolean bothNull = first.isNull() && last.isNull();
        boolean bothSet = !first.isNull() && !last.isNull();
        boolean consistent = length == 0 ? bothNull : bothSet;
        if (!consistent) {
            throw new FspMetadataException("flst base length/endpoints inconsistency on disk: length=" + length
                    + " first.null=" + first.isNull() + " last.null=" + last.isNull());
        }
        return new FlstBase(length, first, last);
    }
}
