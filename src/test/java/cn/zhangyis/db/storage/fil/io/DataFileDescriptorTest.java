package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageNo;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DataFileDescriptor 测试固定表空间文件覆盖范围，后续 PageStore 只能通过该范围判断 page 是否属于文件。
 */
class DataFileDescriptorTest {

    /**
     * 验证 {@code shouldDescribeCoveredPageRange} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldDescribeCoveredPageRange() {
        DataFileDescriptor descriptor = DataFileDescriptor.single(Path.of("t1.ibd"), PageNo.of(64), PageNo.of(32));

        assertEquals(PageNo.of(64), descriptor.startPageNo());
        assertEquals(PageNo.of(32), descriptor.sizeInPages());
        assertEquals(PageNo.of(96), descriptor.endExclusivePageNo());
    }

    /**
     * 验证 {@code shouldRejectEmptyFileRange} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectEmptyFileRange() {
        assertThrows(DatabaseRuntimeException.class, () -> DataFileDescriptor.single(Path.of("t1.ibd"), PageNo.of(0), PageNo.of(0)));
    }
}
