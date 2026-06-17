package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tablespace 领域模型测试固定运行时快照语义，避免后续把 FileChannel 或 FSP 解析职责塞进聚合根。
 */
class TablespaceTest {

    @Test
    void shouldCreateTablespaceSnapshotFromMetadata() {
        Tablespace tablespace = sampleMetadata().toTablespace();

        assertEquals(SpaceId.of(10), tablespace.spaceId());
        assertEquals("user/t1", tablespace.name());
        assertEquals(TablespaceType.FILE_PER_TABLE, tablespace.type());
        assertEquals(TablespaceState.NORMAL, tablespace.state());
        assertEquals(PageSize.ofBytes(16 * 1024), tablespace.pageSize());
        assertEquals(PageNo.of(128), tablespace.currentSizeInPages());
        assertEquals(PageNo.of(128), tablespace.freeLimitPageNo());
    }

    @Test
    void shouldRejectInvalidTablespaceMetadata() {
        DataFileDescriptor dataFile = DataFileDescriptor.single(Path.of("t1.ibd"), PageNo.of(0), PageNo.of(64));

        assertThrows(DatabaseRuntimeException.class, () -> new TablespaceMetadata(null, "t1", TablespaceType.FILE_PER_TABLE,
                PageSize.ofBytes(16 * 1024), TablespaceState.NORMAL, List.of(dataFile), SpaceFlags.empty(), PageNo.of(64), PageNo.of(64), 1));
        assertThrows(DatabaseRuntimeException.class, () -> new TablespaceMetadata(SpaceId.of(1), " ", TablespaceType.FILE_PER_TABLE,
                PageSize.ofBytes(16 * 1024), TablespaceState.NORMAL, List.of(dataFile), SpaceFlags.empty(), PageNo.of(64), PageNo.of(64), 1));
    }

    @Test
    void shouldRejectShrinkingPublishedSize() {
        Tablespace tablespace = sampleMetadata().toTablespace();

        assertThrows(DatabaseRuntimeException.class, () -> tablespace.publishSize(PageNo.of(64), PageNo.of(64)));
    }

    @Test
    void shouldRejectInvalidStateTransition() {
        Tablespace corrupted = sampleMetadata().toTablespace().transitTo(TablespaceState.CORRUPTED);

        assertThrows(DatabaseRuntimeException.class, () -> corrupted.transitTo(TablespaceState.ACTIVE));
    }

    private TablespaceMetadata sampleMetadata() {
        DataFileDescriptor dataFile = DataFileDescriptor.single(Path.of("user", "t1.ibd"), PageNo.of(0), PageNo.of(128));
        return new TablespaceMetadata(SpaceId.of(10), "user/t1", TablespaceType.FILE_PER_TABLE,
                PageSize.ofBytes(16 * 1024), TablespaceState.NORMAL, List.of(dataFile), SpaceFlags.empty(), PageNo.of(128), PageNo.of(128), 1);
    }
}
