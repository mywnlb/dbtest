package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * 页内无符号 16 位大端字段读写助手。INDEX page header 与 PageDirectory 槽、next_record 字段均为 u16
 * （16KB 页内一切偏移 ≤ 65535），但 {@link PageGuard} 只提供 int(4)/long(8) 访问，故此处用 2 字节 readBytes/writeBytes 拼装，
 * 避免写 u16 时用 writeInt 覆盖相邻 2 字节。
 */
final class PageU16 {

    private PageU16() {
    }

    /** 读 at 处 2 字节大端 u16。S/X 均可（委托 PageGuard.readBytes）。 */
    static int get(PageGuard guard, int at) {
        byte[] b = guard.readBytes(at, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    /** 写 at 处 2 字节大端 u16；value 须在 0..65535，否则 {@link DatabaseValidationException}。要求 X（writeBytes 自校验并置脏）。 */
    static void put(PageGuard guard, int at, int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new DatabaseValidationException("u16 page field out of range: " + value);
        }
        guard.writeBytes(at, new byte[]{(byte) (value >>> 8), (byte) value});
    }
}
