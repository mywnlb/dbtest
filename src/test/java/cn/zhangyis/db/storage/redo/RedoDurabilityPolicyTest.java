package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.20a redo DurabilityPolicy + 三阶段 write 原语：append→write(OS cache)→flush(fsync)。
 *
 * <p>验证三种 commit 刷盘策略选用不同 redo 原语：FLUSH_ON_COMMIT 等 fsync、WRITE_ON_COMMIT 只等写到 OS cache、
 * BACKGROUND_FLUSH 不等待（依赖后台 flusher）。本片只到 redo 原语 + 策略，不接生产事务提交路径（2.1）。
 */
class RedoDurabilityPolicyTest {

    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private RedoLogManager durable(String name) {
        return RedoLogManager.durable(RedoLogFileRepository.open(dir.resolve(name)));
    }

    private static LogRange append(RedoLogManager redo) {
        return redo.append(List.of(new PageBytesRecord(P, 200, new byte[]{1, 2, 3})));
    }

    @Test
    void flushOnCommitMakesCommitLsnDurable() {
        RedoLogManager redo = durable("flush.log");
        Lsn commitLsn = append(redo).end();

        assertTrue(DurabilityPolicy.FLUSH_ON_COMMIT.awaitCommitDurable(redo, commitLsn, Duration.ofMillis(200)));
        assertTrue(redo.flushedToDiskLsn().value() >= commitLsn.value(), "FLUSH_ON_COMMIT 等到 fsync durable");
    }

    @Test
    void writeOnCommitAdvancesWrittenButNotFlushed() {
        RedoLogManager redo = durable("write.log");
        Lsn commitLsn = append(redo).end();

        assertTrue(DurabilityPolicy.WRITE_ON_COMMIT.awaitCommitDurable(redo, commitLsn, Duration.ofMillis(200)));
        assertTrue(redo.writtenToDiskLsn().value() >= commitLsn.value(), "WRITE_ON_COMMIT 写到 OS cache");
        assertEquals(Lsn.of(0), redo.flushedToDiskLsn(), "WRITE_ON_COMMIT 不 fsync，flushedLsn 不前进");
    }

    @Test
    void backgroundFlushDoesNotWaitOnCommit() {
        RedoLogManager redo = durable("bg.log");
        Lsn commitLsn = append(redo).end();

        assertTrue(DurabilityPolicy.BACKGROUND_FLUSH.awaitCommitDurable(redo, commitLsn, Duration.ofMillis(200)));
        assertEquals(Lsn.of(0), redo.writtenToDiskLsn(), "BACKGROUND_FLUSH 不在 commit 写盘");
        assertEquals(Lsn.of(0), redo.flushedToDiskLsn());

        // 后台 flusher 的等价动作：一次 flush 后才 durable。
        redo.flush();
        assertTrue(redo.flushedToDiskLsn().value() >= commitLsn.value());
    }

    @Test
    void waitWrittenReturnsWhenWritten() {
        RedoLogManager redo = durable("ww.log");
        Lsn commitLsn = append(redo).end();
        redo.write();
        assertTrue(redo.waitWritten(commitLsn, Duration.ofMillis(100)));
    }

    @Test
    void waitWrittenTimesOutWhenNoWriter() {
        RedoLogManager redo = durable("ww-timeout.log");
        Lsn commitLsn = append(redo).end();
        // 未调用 write()/flush()，redo 仍只在内存 pending → 等待 OS-cache 写入超时。
        assertFalse(redo.waitWritten(commitLsn, Duration.ofMillis(10)));
        assertEquals(Lsn.of(0), redo.writtenToDiskLsn());
    }

    @Test
    void writtenToDiskLsnMonotonicAcrossWriteThenFlush() {
        RedoLogManager redo = durable("mono.log");
        Lsn e1 = append(redo).end();
        redo.write();
        assertEquals(e1, redo.writtenToDiskLsn());
        assertEquals(Lsn.of(0), redo.flushedToDiskLsn(), "write 不推进 flushed");

        Lsn e2 = append(redo).end();
        redo.flush();
        assertEquals(e2, redo.writtenToDiskLsn(), "flush 也推进 written，且不回退");
        assertEquals(e2, redo.flushedToDiskLsn(), "flush 后 flushed 追上 written");
        assertTrue(redo.flushedToDiskLsn().value() <= redo.writtenToDiskLsn().value(), "flushed <= written 不变量");
    }
}
