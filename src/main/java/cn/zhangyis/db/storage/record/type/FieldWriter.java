package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 字段顺序写入器：从 backing 的 start 处顺序写字节。供 codec 编码，不依赖 buf/页。
 */
public final class FieldWriter {

    private final byte[] backing;
    private final int start;
    private int pos;

    public FieldWriter(byte[] backing, int start) {
        if (backing == null) {
            throw new DatabaseValidationException("field writer backing must not be null");
        }
        if (start < 0 || start > backing.length) {
            throw new DatabaseValidationException("field writer start out of range: " + start);
        }
        this.backing = backing;
        this.start = start;
    }

    /** 写一字节（取低 8 位）。 */
    public void putByte(int b) {
        backing[start + pos] = (byte) b;
        pos++;
    }

    /** 写一段字节。 */
    public void putBytes(byte[] src) {
        if (src == null) {
            throw new DatabaseValidationException("write source must not be null");
        }
        System.arraycopy(src, 0, backing, start + pos, src.length);
        pos += src.length;
    }

    /** 已写字节数。 */
    public int written() {
        return pos;
    }
}
