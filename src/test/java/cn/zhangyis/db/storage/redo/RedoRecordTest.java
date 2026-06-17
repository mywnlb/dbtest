package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** RedoRecord：PageBytesRecord 防御性 copy（构造 clone + accessor clone），byteLength 与 R1 文件编码保持一致。 */
class RedoRecordTest {

    private static final PageId PID = PageId.of(SpaceId.of(1), PageNo.of(3));

    @Test
    void pageBytesRecordDefensivelyCopies() {
        byte[] src = {1, 2, 3};
        PageBytesRecord r = new PageBytesRecord(PID, 98, src);
        src[0] = 9; // 改外部数组不应影响记录
        assertArrayEquals(new byte[]{1, 2, 3}, r.bytes());
        assertNotSame(r.bytes(), r.bytes(), "accessor returns fresh clone each call");
        // tag(1)+spaceId(4)+pageNo(8)+offset(4)+payloadLen(4)+payload(3)
        assertEquals(24, r.byteLength());
    }

    @Test
    void pageInitRecordLengthMatchesFileEncoding() {
        PageInitRecord r = new PageInitRecord(PID, cn.zhangyis.db.storage.page.PageType.INDEX);
        // tag(1)+spaceId(4)+pageNo(8)+pageType(4)
        assertEquals(17, r.byteLength());
    }
}
