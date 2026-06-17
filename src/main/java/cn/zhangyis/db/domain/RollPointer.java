package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * DB_ROLL_PTR：指向 undo record 的位置。本片（T1.1+T1.2）只用 {@link #NULL}（尚无 undo）。
 *
 * <p>7 字节定长编码（big-endian，与 InnoDB 二进制不兼容，故意简化）：
 * <ul>
 *   <li>byte0：bit7 = insert flag；bit6..0 = reserved，必须为 0；</li>
 *   <li>byte1..4：pageNo（u32）；</li>
 *   <li>byte5..6：offset（u16）。</li>
 * </ul>
 * space id 不入编码，由 undo 表空间隐含（首个 undo 片单 undo 表空间假设；多 undo 表空间需改为 InnoDB 风格
 * rollback-segment-id 编码，留 T1.3）。{@link #NULL} 为全零（undo page 0 保留作头页，故全零与任何真实 undo
 * 记录位置无歧义）。
 *
 * @param insert 是否 insert undo（true=insert，false=update/delete）。
 * @param pageNo undo page 号，必须落在 u32 范围。
 * @param offset undo record 页内偏移，必须落在 u16 范围。
 */
public record RollPointer(boolean insert, PageNo pageNo, int offset) {

    private static final long U32_MAX = 0xFFFFFFFFL;
    private static final int U16_MAX = 0xFFFF;
    private static final int INSERT_BIT = 0x80;
    private static final int RESERVED_MASK = 0x7F;

    /** 编码字节宽度。 */
    public static final int BYTES = 7;

    /** 全零空指针哨兵（无 undo）。 */
    public static final RollPointer NULL = new RollPointer(false, PageNo.of(0), 0);

    public RollPointer {
        if (pageNo == null) {
            throw new DatabaseValidationException("roll pointer pageNo must not be null");
        }
        if (pageNo.value() < 0 || pageNo.value() > U32_MAX) {
            throw new DatabaseValidationException("roll pointer pageNo out of u32: " + pageNo.value());
        }
        if (offset < 0 || offset > U16_MAX) {
            throw new DatabaseValidationException("roll pointer offset out of u16: " + offset);
        }
    }

    /** 是否空指针（无 undo）：insert=false 且 pageNo=0 且 offset=0。 */
    public boolean isNull() {
        return !insert && pageNo.value() == 0 && offset == 0;
    }

    /** 编码为 7 字节（big-endian）。 */
    public byte[] encode() {
        byte[] b = new byte[BYTES];
        b[0] = (byte) (insert ? INSERT_BIT : 0);
        long p = pageNo.value();
        b[1] = (byte) (p >>> 24);
        b[2] = (byte) (p >>> 16);
        b[3] = (byte) (p >>> 8);
        b[4] = (byte) p;
        b[5] = (byte) (offset >>> 8);
        b[6] = (byte) offset;
        return b;
    }

    /**
     * 从 {@code off} 处解码 7 字节。reserved 位非 0 视为损坏抛 {@link DatabaseValidationException}，
     * 不静默忽略——否则未来扩展位被误吞会破坏向后兼容判断。
     */
    public static RollPointer decode(byte[] buf, int off) {
        if (buf == null || off < 0 || off + BYTES > buf.length) {
            throw new DatabaseValidationException("roll pointer buffer too short");
        }
        int flags = buf[off] & 0xFF;
        if ((flags & RESERVED_MASK) != 0) {
            throw new DatabaseValidationException("roll pointer reserved bits set: " + flags);
        }
        boolean insert = (flags & INSERT_BIT) != 0;
        long p = ((long) (buf[off + 1] & 0xFF) << 24)
                | ((long) (buf[off + 2] & 0xFF) << 16)
                | ((long) (buf[off + 3] & 0xFF) << 8)
                | (buf[off + 4] & 0xFF);
        int offset = ((buf[off + 5] & 0xFF) << 8) | (buf[off + 6] & 0xFF);
        return new RollPointer(insert, PageNo.of(p), offset);
    }
}
