package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.PageOutOfBoundsException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * FileChannelPageStore 测试固定门面路由与生命周期，不依赖 TablespaceRegistry（物理层 registry-无关、state-无关）。
 */
class FileChannelPageStoreTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    @Test
    void shouldRoundTripThroughFacade() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("a.ibd"), PS, PageNo.of(4));
            byte[] payload = new byte[PS.bytes()];
            Arrays.fill(payload, (byte) 0x5C);
            store.writePage(PageId.of(space, PageNo.of(1)), ByteBuffer.wrap(payload));

            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(space, PageNo.of(1)), dst);
            assertArrayEquals(payload, dst.array());
        }
    }

    @Test
    void shouldExtendThroughFacadeAndExposeNewSize() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("b.ibd"), PS, PageNo.of(1));
            // 初始 1 页 < 1 extent（16KB → 64 页），默认策略本次增量为 1 页 → 新 size = 2。
            PageNo newSize = store.extend(space);
            assertEquals(PageNo.of(2), newSize);
            assertEquals(PageNo.of(2), store.currentSizeInPages(space));

            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(space, PageNo.of(1)), dst);
        }
    }

    @Test
    void shouldReopenExistingFileThroughFacadeAndReadBack() {
        SpaceId space = SpaceId.of(5);
        Path path = dir.resolve("reopen.ibd");
        byte[] payload = new byte[PS.bytes()];
        Arrays.fill(payload, (byte) 0x3A);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, path, PS, PageNo.of(2));
            store.writePage(PageId.of(space, PageNo.of(0)), ByteBuffer.wrap(payload));
            store.close(space);

            // 经门面重新 open 已存在文件：size 由文件长度推导，且先前写入的数据可读回。
            store.open(space, path, PS);
            assertEquals(PageNo.of(2), store.currentSizeInPages(space));
            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(space, PageNo.of(0)), dst);
            assertArrayEquals(payload, dst.array());
        }
    }

    @Test
    void shouldRejectIoOnUnopenedSpace() {
        try (PageStore store = new FileChannelPageStore()) {
            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            assertThrows(TablespaceNotOpenException.class,
                    () -> store.readPage(PageId.of(SpaceId.of(99), PageNo.of(0)), dst));
        }
    }

    @Test
    void pathOfReturnsOpenFilePath() {
        SpaceId space = SpaceId.of(31);
        Path path = dir.resolve("pathof.ibd");
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, path, PS, PageNo.of(8));

            assertEquals(path, store.pathOf(space));
        }
    }

    @Test
    void pathOfRejectsUnopenedSpace() {
        try (PageStore store = new FileChannelPageStore()) {
            assertThrows(TablespaceNotOpenException.class, () -> store.pathOf(SpaceId.of(99)));
        }
    }

    @Test
    void shouldRejectDuplicateRegistration() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("c.ibd"), PS, PageNo.of(1));
            assertThrows(DatabaseValidationException.class,
                    () -> store.create(space, dir.resolve("c2.ibd"), PS, PageNo.of(1)));
        }
    }

    @Test
    void shouldRejectIoAfterClose() {
        SpaceId space = SpaceId.of(3);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("d.ibd"), PS, PageNo.of(2));
            store.close(space);
            ByteBuffer dst = ByteBuffer.allocate(PS.bytes());
            assertThrows(TablespaceNotOpenException.class,
                    () -> store.readPage(PageId.of(space, PageNo.of(0)), dst));
        }
    }

    /**
     * 物理截断必须同时缩短文件、发布新页数，并让旧尾页立即变为越界，防止恢复后继续访问 stale tail。
     */
    @Test
    void shouldPhysicallyTruncateAndPublishSmallerSize() throws Exception {
        SpaceId space = SpaceId.of(32);
        Path path = dir.resolve("truncate.ibu");
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, path, PS, PageNo.of(4));

            store.truncate(space, PageNo.of(2));

            assertEquals(PageNo.of(2), store.currentSizeInPages(space));
            assertEquals(2L * PS.bytes(), Files.size(path));
            assertThrows(PageOutOfBoundsException.class,
                    () -> store.readPage(PageId.of(space, PageNo.of(2)), ByteBuffer.allocate(PS.bytes())));
        }
    }

    @Test
    void shouldRejectNonShrinkingPhysicalTruncate() {
        SpaceId space = SpaceId.of(33);
        try (PageStore store = new FileChannelPageStore()) {
            store.create(space, dir.resolve("not-shrink.ibu"), PS, PageNo.of(4));
            assertThrows(DatabaseValidationException.class,
                    () -> store.truncate(space, PageNo.of(4)));
            assertThrows(DatabaseValidationException.class,
                    () -> store.truncate(space, PageNo.of(5)));
        }
    }
}
