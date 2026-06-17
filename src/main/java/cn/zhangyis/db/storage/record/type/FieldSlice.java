package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * 字段字节只读视图（backing 的 [offset, offset+length)）。供 codec 解码与比较，不依赖 buf/页。
 *
 * @param backing 承载数组（不复制，调用方保证生命周期）。
 * @param offset  起始偏移。
 * @param length  长度。
 */
public record FieldSlice(byte[] backing, int offset, int length) {

    public FieldSlice {
        if (backing == null) {
            throw new DatabaseValidationException("field slice backing must not be null");
        }
        if (offset < 0 || length < 0 || offset + length > backing.length) {
            throw new DatabaseValidationException("field slice out of bounds: offset=" + offset
                    + " length=" + length + " backing=" + backing.length);
        }
    }

    /** 第 i 字节（无符号 0..255）。 */
    public int byteAt(int i) {
        if (i < 0 || i >= length) {
            throw new DatabaseValidationException("field slice index out of range: " + i);
        }
        return backing[offset + i] & 0xFF;
    }

    /** 复制本切片字节。 */
    public byte[] copyBytes() {
        return Arrays.copyOfRange(backing, offset, offset + length);
    }

    /** 无符号字典序比较两个切片（保序编码下 = 类型自然序）。 */
    public static int compareUnsigned(FieldSlice a, FieldSlice b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = a.byteAt(i) - b.byteAt(i);
            if (d != 0) {
                return d;
            }
        }
        return Integer.compare(a.length, b.length);
    }
}
