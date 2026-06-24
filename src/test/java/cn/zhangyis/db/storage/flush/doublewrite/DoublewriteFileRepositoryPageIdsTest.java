package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * doublewrite 文件页枚举：恢复期"待检查页列表"来源。返回有有效 slot（CRC + 页 checksum 通过）的去重 PageId；
 * 跳过页 checksum 不通过的无效 slot。
 */
class DoublewriteFileRepositoryPageIdsTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void pageIdsReturnsDistinctValidSlotPages() {
        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            PageId p2 = PageId.of(SPACE, PageNo.of(2));
            PageId p5 = PageId.of(SPACE, PageNo.of(5));
            dw.append(stamped(p2, 10));
            dw.append(stamped(p5, 11));
            dw.append(stamped(p2, 12)); // 同页再次写：必须去重

            assertEquals(Set.of(p2, p5), Set.copyOf(dw.pageIds()));
        }
    }

    @Test
    void pageIdsSkipsSlotsFailingPageChecksum() {
        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            PageId valid = PageId.of(SPACE, PageNo.of(2));
            PageId bad = PageId.of(SPACE, PageNo.of(9));
            dw.append(stamped(valid, 10));
            dw.append(unstamped(bad, 11)); // 页 checksum 不通过的 slot

            assertEquals(Set.of(valid), Set.copyOf(dw.pageIds()));
        }
    }

    /** 构造页 checksum 通过的 slot 镜像。 */
    private static FlushPageSnapshot stamped(PageId pageId, long pageLsn) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer.wrap(image).putLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
        PageImageChecksum.stamp(image, PS);
        return new FlushPageSnapshot(pageId, Lsn.of(pageLsn), 1L, image);
    }

    /** 构造未盖 checksum 的 slot 镜像（页 checksum 校验不通过）。 */
    private static FlushPageSnapshot unstamped(PageId pageId, long pageLsn) {
        return new FlushPageSnapshot(pageId, Lsn.of(pageLsn), 1L, new byte[PS.bytes()]);
    }
}
