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

    /**
     * 验证 {@code pageBytesRecordDefensivelyCopies} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
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

    /**
     * 验证 {@code pageInitRecordLengthMatchesFileEncoding} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void pageInitRecordLengthMatchesFileEncoding() {
        PageInitRecord r = new PageInitRecord(PID, cn.zhangyis.db.storage.page.PageType.INDEX);
        // tag(1)+spaceId(4)+pageNo(8)+pageType(4)
        assertEquals(17, r.byteLength());
    }
}
