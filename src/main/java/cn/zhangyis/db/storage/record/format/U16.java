package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 记录格式内部的 2 字节无符号大端整数读写工具（0..65535）。 */
final class U16 {

    private U16() {
    }

    static void put(byte[] buf, int at, int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new DatabaseValidationException("u16 out of range: " + value);
        }
        buf[at] = (byte) ((value >>> 8) & 0xFF);
        buf[at + 1] = (byte) (value & 0xFF);
    }

    static int get(byte[] buf, int at) {
        return ((buf[at] & 0xFF) << 8) | (buf[at + 1] & 0xFF);
    }
}
