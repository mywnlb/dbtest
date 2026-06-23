package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SpaceHeaderRawCodec 测试：经 SpaceHeaderRepository 写 page0 并刷盘后，raw 字节解出的物理字段应与写入一致。
 */
class SpaceHeaderRawCodecTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(42);

    @TempDir
    Path dir;

    @Test
    void readPhysicalParsesWrittenHeader() {
        Path path = dir.resolve("hdr.ibd");
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 64)) {
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager mgr = new MiniTransactionManager();
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            MiniTransaction mtr = mgr.begin();
            headerRepo.initialize(mtr, new SpaceHeaderSnapshot(SPACE, PS, 0xABC, PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 7L));
            mgr.commit(mtr);
        }

        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            ByteBuffer page = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(SPACE, PageNo.of(0)), page);

            SpaceHeaderPhysical physical = SpaceHeaderRawCodec.readPhysical(page);

            assertEquals(SPACE, physical.spaceId());
            assertEquals(PS, physical.pageSize());
            assertEquals(0xABC, physical.spaceFlags());
            assertEquals(64L, physical.currentSizeInPages().value());
            assertEquals(0L, physical.freeLimitPageNo().value());
            assertEquals(7L, physical.spaceVersion());
        }
    }
}
