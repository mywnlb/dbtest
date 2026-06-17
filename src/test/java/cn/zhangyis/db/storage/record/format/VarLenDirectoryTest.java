package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 变长目录：条目数/长度访问、字节数、往返一致、u16 范围校验。 */
class VarLenDirectoryTest {

    /** 每条 2 字节，count 与 length 反映构造数组。 */
    @Test
    void exposesCountAndLengths() {
        VarLenDirectory dir = new VarLenDirectory(new int[]{0, 255, 65535});
        assertEquals(3, dir.count());
        assertEquals(6, dir.byteLength());
        assertEquals(0, dir.length(0));
        assertEquals(255, dir.length(1));
        assertEquals(65535, dir.length(2));
    }

    /** 写出后按相同条数读回，各长度一致。 */
    @Test
    void roundTripThroughBuffer() {
        int[] lens = {3, 128, 4096};
        VarLenDirectory dir = new VarLenDirectory(lens);
        byte[] buf = new byte[dir.byteLength()];
        dir.writeTo(buf, 0);
        VarLenDirectory back = VarLenDirectory.readFrom(buf, 0, lens.length);
        int[] got = new int[lens.length];
        for (int i = 0; i < lens.length; i++) {
            got[i] = back.length(i);
        }
        assertArrayEquals(lens, got);
    }

    /** 空目录占 0 字节。 */
    @Test
    void emptyDirectory() {
        VarLenDirectory dir = new VarLenDirectory(new int[0]);
        assertEquals(0, dir.count());
        assertEquals(0, dir.byteLength());
    }

    /** 长度超出 u16 或为负应被拒绝；构造入参为空亦拒绝。 */
    @Test
    void rejectsOutOfRange() {
        assertThrows(DatabaseValidationException.class, () -> new VarLenDirectory(new int[]{0x10000}));
        assertThrows(DatabaseValidationException.class, () -> new VarLenDirectory(new int[]{-1}));
        assertThrows(DatabaseValidationException.class, () -> new VarLenDirectory(null));
    }
}
