package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * recovery progress journal 持久化测试：文件只用于诊断，不参与恢复阶段跳过或状态决策。
 */
class RecoveryProgressJournalTest {

    @TempDir
    Path dir;

    @Test
    void persistentJournalAppendsJsonLinesInSequence() throws Exception {
        Path path = dir.resolve("nested").resolve("recovery-progress.jsonl");
        RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(path);

        journal.stageStarted(RecoveryMode.NORMAL, RecoveryStageName.TRAFFIC_CLOSED);
        journal.stageCompleted(RecoveryMode.NORMAL, RecoveryStageName.OPEN_TRAFFIC,
                RecoveryState.OPEN, Lsn.of(42));

        List<String> lines = Files.readAllLines(path);
        assertEquals(2, lines.size());
        assertTrue(lines.get(0).contains("\"sequence\":1"));
        assertTrue(lines.get(0).contains("\"mode\":\"NORMAL\""));
        assertTrue(lines.get(0).contains("\"stageName\":\"TRAFFIC_CLOSED\""));
        assertTrue(lines.get(0).contains("\"kind\":\"STARTED\""));
        assertTrue(lines.get(1).contains("\"sequence\":2"));
        assertTrue(lines.get(1).contains("\"stageName\":\"OPEN_TRAFFIC\""));
        assertTrue(lines.get(1).contains("\"state\":\"OPEN\""));
        assertTrue(lines.get(1).contains("\"recoveredToLsn\":42"));
    }

    @Test
    void persistentJournalEscapesFailureDetail() throws Exception {
        Path path = dir.resolve("recovery-progress.jsonl");
        RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(path);

        journal.stageFailed(RecoveryMode.NORMAL, RecoveryStageName.REDO_REPLAY,
                new DatabaseRuntimeException("bad \"redo\"\nline"));

        String line = Files.readString(path);
        assertTrue(line.contains("\"kind\":\"FAILED\""));
        assertTrue(line.contains("bad \\\"redo\\\"\\nline"));
    }

    @Test
    void persistentJournalWritesCompletedDetail() throws Exception {
        Path path = dir.resolve("force-skip-progress.jsonl");
        RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(path);

        journal.stageCompleted(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE,
                RecoveryStageName.OPEN_TRAFFIC, RecoveryState.OPEN, Lsn.of(77),
                "skippedSpaces=[7], skippedRedoRecords=3");

        String line = Files.readString(path);
        assertTrue(line.contains("\"kind\":\"COMPLETED\""));
        assertTrue(line.contains("\"mode\":\"FORCE_SKIP_CORRUPT_TABLESPACE\""));
        assertTrue(line.contains("skippedSpaces=[7]"));
        assertTrue(line.contains("skippedRedoRecords=3"));
    }
}
