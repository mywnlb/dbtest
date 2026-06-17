package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 记录头编解码：flags 位打包、u16/u8 范围、writeTo/readFrom 往返一致。 */
class RecordHeaderTest {

    /** 头部写出后读回应当字段完全一致，验证 flags 与各偏移布局。 */
    @Test
    void roundTripPreservesAllFields() {
        RecordHeader h = new RecordHeader(true, true, RecordType.NODE_POINTER, 1234, 200, 4096, 65535);
        byte[] buf = new byte[RecordHeaderLayout.SIZE];
        h.writeTo(buf, 0);
        RecordHeader back = RecordHeader.readFrom(buf, 0);
        assertEquals(h, back);
    }

    /** flags 字节低 2 位为 delete/minRec，高位承载 recordType code。 */
    @Test
    void flagsPackDeleteMinRecAndType() {
        byte[] buf = new byte[RecordHeaderLayout.SIZE];
        new RecordHeader(false, false, RecordType.CONVENTIONAL, 0, 0, 0, 8).writeTo(buf, 0);
        assertEquals(0, buf[RecordHeaderLayout.FLAGS] & 0xFF);

        new RecordHeader(true, false, RecordType.SUPREMUM, 0, 0, 0, 8).writeTo(buf, 0);
        int flags = buf[RecordHeaderLayout.FLAGS] & 0xFF;
        assertTrue((flags & 1) != 0);
        assertFalse((flags & 2) != 0);
        assertEquals(RecordType.SUPREMUM.code(), (flags >> 2) & 0x3);
    }

    /** 各 u16 字段越界（负或 >65535）应被构造校验拒绝。 */
    @Test
    void rejectsOutOfRangeFields() {
        assertThrows(DatabaseValidationException.class,
                () -> new RecordHeader(false, false, RecordType.CONVENTIONAL, 0x10000, 0, 0, 8));
        assertThrows(DatabaseValidationException.class,
                () -> new RecordHeader(false, false, RecordType.CONVENTIONAL, 0, 256, 0, 8));
        assertThrows(DatabaseValidationException.class,
                () -> new RecordHeader(false, false, RecordType.CONVENTIONAL, 0, 0, -1, 8));
    }

    /** recordType 不能为空。 */
    @Test
    void rejectsNullRecordType() {
        assertThrows(DatabaseValidationException.class,
                () -> new RecordHeader(false, false, null, 0, 0, 0, 8));
    }

    /** RecordType code 往返；未知 code 视为记录头损坏。 */
    @Test
    void recordTypeFromCodeRejectsUnknown() {
        for (RecordType t : RecordType.values()) {
            assertEquals(t, RecordType.fromCode(t.code()));
        }
        assertThrows(RecordFormatException.class, () -> RecordType.fromCode(7));
    }
}
