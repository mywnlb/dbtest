package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * F1 data file force 测试：FlushCoordinator 写 data file 后需要明确 fsync 边界。
 */
class PageStoreForceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    /**
     * 验证 {@code forceExistingTablespaceSucceedsAndUnknownTablespaceFails} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void forceExistingTablespaceSucceedsAndUnknownTablespaceFails() {
        try (PageStore store = new FileChannelPageStore()) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            store.force(SPACE);
            assertThrows(TablespaceNotOpenException.class, () -> store.force(SpaceId.of(99)));
            assertThrows(DatabaseValidationException.class, () -> store.force(null));
        }
    }
}
