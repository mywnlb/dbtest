package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** recovery 后 redo 续写测试：新 manager 安装 recoveredTo 后追加，文件批次 LSN 必须连续且可再次扫描。 */
class RedoRecoveryContinuationTest {

    @TempDir
    Path dir;

    @Test
    void appendsAfterRecoveredBoundaryWithoutOverlappingExistingLog() {
        Path path = dir.resolve("redo.log");
        PageId page = PageId.of(SpaceId.of(1), PageNo.of(1));
        LogRange first;
        try (RedoLogFileRepository repository = RedoLogFileRepository.open(path)) {
            RedoLogManager original = RedoLogManager.durable(repository);
            first = original.append(List.of(new PageInitRecord(page, PageType.INDEX)));
            original.flush();
        }
        try (RedoLogFileRepository repository = RedoLogFileRepository.open(path)) {
            RedoLogManager recovered = RedoLogManager.durable(repository);
            recovered.restoreRecoveredBoundary(first.end());
            assertEquals(first.end(), recovered.flush(),
                    "empty flush after recovery must not move durable LSN backwards");
            LogRange second = recovered.append(List.of(new PageBytesRecord(page, 100, new byte[]{1})));
            recovered.flush();
            assertEquals(first.end(), second.start());
        }
        try (RedoLogFileRepository repository = RedoLogFileRepository.open(path)) {
            List<RedoLogBatch> batches = repository.readBatches();
            assertEquals(2, batches.size());
            assertEquals(batches.get(0).range().end(), batches.get(1).range().start());
        }
    }
}
