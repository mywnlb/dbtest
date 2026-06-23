package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * doublewrite recovery 边界加固（F3）：越当前物理文件尾的页不得越界读取或被 doublewrite 复活，
 * 直接跳过，交给 redo replay 的 extend-on-demand 与 SPACE_FILE_RECONCILE 重建。
 */
class DoublewriteRecoveryScannerBoundsTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void skipsPageBeyondCurrentFileSize() {
        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4)); // 仅 0..3 物理存在
            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(dw, store, PS);

            assertFalse(scanner.repairPageIfNeeded(PageId.of(SPACE, PageNo.of(70))),
                    "越界页必须被跳过，不得越界读取");
        }
    }
}
