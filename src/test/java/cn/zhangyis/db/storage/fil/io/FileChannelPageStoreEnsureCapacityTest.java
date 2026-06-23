package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link PageStore#ensureCapacity} 单元测试：crash recovery 把物理文件重对齐到 page0 逻辑大小时使用。
 *
 * <p>它是 {@code truncateTo} 的镜像——只增不减、幂等：目标小于等于当前大小为 no-op，目标更大则零填充新增页。
 */
class FileChannelPageStoreEnsureCapacityTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    /** 目标大于当前物理大小：扩展并对新增页零填充，扩展后新页可读且内容为零。 */
    @Test
    void growsAndZeroFillsWhenShort() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            store.ensureCapacity(SPACE, PageNo.of(10));
            assertEquals(10L, store.currentSizeInPages(SPACE).value());
            byte[] page = new byte[PS.bytes()];
            store.readPage(PageId.of(SPACE, PageNo.of(9)), ByteBuffer.wrap(page));
            assertEquals(0, page[100]);
        }
    }

    /** 目标小于等于当前大小：幂等 no-op，绝不缩短文件。 */
    @Test
    void noOpWhenAlreadyLargeEnough() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
            store.ensureCapacity(SPACE, PageNo.of(8));
            store.ensureCapacity(SPACE, PageNo.of(3));
            assertEquals(8L, store.currentSizeInPages(SPACE).value());
        }
    }

    /** 非正目标页数非法：crash recovery 永远以 page0 正大小调用，0/负值表示损坏，必须拒绝。 */
    @Test
    void rejectsNonPositiveTarget() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            assertThrows(DatabaseValidationException.class, () -> store.ensureCapacity(SPACE, PageNo.of(0)));
        }
    }
}
