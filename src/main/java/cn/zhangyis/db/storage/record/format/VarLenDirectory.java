package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 变长字段目录（innodb-record-design §5.2）。按非 NULL 变长列的列序，每列 2 字节长度（u16）。
 */
public final class VarLenDirectory {

    private final int[] lengths;

    public VarLenDirectory(int[] lengths) {
        if (lengths == null) {
            throw new DatabaseValidationException("var len directory lengths must not be null");
        }
        for (int len : lengths) {
            if (len < 0 || len > 0xFFFF) {
                throw new DatabaseValidationException("var field length out of u16 range: " + len);
            }
        }
        this.lengths = lengths.clone();
    }

    /** 条目数（= 非 NULL 变长列数）。 */
    public int count() {
        return lengths.length;
    }

    /** 第 i 条长度。 */
    public int length(int i) {
        return lengths[i];
    }

    /** 目录字节数。 */
    public int byteLength() {
        return lengths.length * 2;
    }

    /** 写目录到 buf 的 at 处。 */
    public void writeTo(byte[] buf, int at) {
        for (int i = 0; i < lengths.length; i++) {
            U16.put(buf, at + i * 2, lengths[i]);
        }
    }

    /** 从 buf 的 at 处读 count 条目录。 */
    public static VarLenDirectory readFrom(byte[] buf, int at, int count) {
        int[] l = new int[count];
        for (int i = 0; i < count; i++) {
            l[i] = U16.get(buf, at + i * 2);
        }
        return new VarLenDirectory(l);
    }
}
