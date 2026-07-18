package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 表空间外部文件 identity 的值域和 SpaceId 绑定测试。 */
class TablespaceFileIdentityTest {
    /**
     * 验证 {@code rejectsMissingOrNonPositiveIdentityFields} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsMissingOrNonPositiveIdentityFields() {
        assertThrows(DatabaseValidationException.class, () ->
                new TablespaceFileIdentity(null, PageSize.ofBytes(4096), TablespaceType.GENERAL, 1, 1));
        assertThrows(DatabaseValidationException.class, () ->
                new TablespaceFileIdentity(SpaceId.of(1), PageSize.ofBytes(4096), TablespaceType.GENERAL, 0, 1));
    }

    /**
     * 验证 {@code preservesStableIdentityComponents} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void preservesStableIdentityComponents() {
        TablespaceFileIdentity identity = new TablespaceFileIdentity(
                SpaceId.of(7), PageSize.ofBytes(4096), TablespaceType.GENERAL, 80046, 3);
        assertEquals(SpaceId.of(7), identity.spaceId());
        assertEquals(3, identity.spaceVersion());
    }
}
