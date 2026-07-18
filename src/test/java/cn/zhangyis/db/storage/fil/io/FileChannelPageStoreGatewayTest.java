package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FileChannelPageStore gateway 接线测试：默认构造器仍必须保持跨平台零填充语义。
 */
class FileChannelPageStoreGatewayTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(9);

    @TempDir
    Path dir;

    /**
     * 验证 {@code defaultConstructorKeepsZeroFillBehavior} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void defaultConstructorKeepsZeroFillBehavior() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(SPACE, dir.resolve("zero-fill.ibd"), PS, PageNo.of(1));
            store.ensureCapacity(SPACE, PageNo.of(4));

            byte[] page = new byte[PS.bytes()];
            store.readPage(PageId.of(SPACE, PageNo.of(3)), ByteBuffer.wrap(page));

            for (byte b : page) {
                assertEquals(0, b);
            }
        }
    }
}
