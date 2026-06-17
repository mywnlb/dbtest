package cn.zhangyis.db.storage.record.page;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 插入方向 code 往返；未知 code 视为 page header 损坏。 */
class IndexPageDirectionTest {

    @Test
    void codeRoundTrip() {
        for (IndexPageDirection d : IndexPageDirection.values()) {
            assertEquals(d, IndexPageDirection.fromCode(d.code()));
        }
    }

    @Test
    void unknownCodeRejected() {
        assertThrows(PageDirectoryCorruptedException.class, () -> IndexPageDirection.fromCode(99));
    }
}
