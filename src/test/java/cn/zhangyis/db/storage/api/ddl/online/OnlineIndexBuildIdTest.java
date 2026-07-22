package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Online build identity 必须保持正值、值语义和无符号无关的稳定升序。 */
class OnlineIndexBuildIdTest {

    /** 多日志 force 使用 build id 排序，比较结果必须与持久正 long identity 一致。 */
    @Test
    void shouldSortByStableIdentity() {
        List<OnlineIndexBuildId> ids = new java.util.ArrayList<>(List.of(
                OnlineIndexBuildId.of(9), OnlineIndexBuildId.of(2), OnlineIndexBuildId.of(5)));

        ids.sort(null);

        assertEquals(List.of(OnlineIndexBuildId.of(2), OnlineIndexBuildId.of(5),
                OnlineIndexBuildId.of(9)), ids);
    }

    /** 零值和负值不能伪装成尚未绑定或有效 DDL identity。 */
    @Test
    void shouldRejectNonPositiveIdentity() {
        assertThrows(DatabaseValidationException.class, () -> OnlineIndexBuildId.of(0));
        assertThrows(DatabaseValidationException.class, () -> OnlineIndexBuildId.of(-1));
    }
}
