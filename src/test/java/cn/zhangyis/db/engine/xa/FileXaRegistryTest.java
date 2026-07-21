package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecision;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;

import static org.junit.jupiter.api.Assertions.*;

/**
 * `mysql.xa` frame、状态机与恢复决议验收。
 */
class FileXaRegistryTest {

    @TempDir
    Path directory;

    /**
     * 完整状态链跨 reopen 保持 XID/transaction 映射；PREPARING 与 durable decision
     * 必须分别产生 rollback/commit 恢复决议。
     */
    @Test
    void persistsStateChainAndRecoveryDecisionsAcrossReopen() {
        XaId first = new XaId(7, new byte[]{1, 2}, new byte[]{3});
        XaId second = new XaId(-9, new byte[]{4}, new byte[0]);
        try (FileXaRegistry registry = FileXaRegistry.openOrCreate(directory)) {
            registry.preparing(first, TransactionId.of(11));
            assertEquals(PreparedTransactionDecision.ROLLBACK,
                    registry.decisionFor(TransactionId.of(11)));
            registry.prepared(first, TransactionId.of(11));
            assertEquals(PreparedTransactionDecision.UNRESOLVED,
                    registry.decisionFor(TransactionId.of(11)));
            registry.decideCommit(first, TransactionId.of(11));
            registry.preparing(second, TransactionId.of(12));
        }

        try (FileXaRegistry reopened = FileXaRegistry.openOrCreate(directory)) {
            assertEquals(PreparedTransactionDecision.COMMIT,
                    reopened.decisionFor(TransactionId.of(11)));
            assertEquals(PreparedTransactionDecision.ROLLBACK,
                    reopened.decisionFor(TransactionId.of(12)));
            assertEquals(2, reopened.completeRecoveryDecisions());
            assertTrue(reopened.pendingEntries().isEmpty());
        }
    }

    /**
     * torn tail 可在 instance lock 所代表的独占前提下截断；完整 frame 的 CRC 损坏不能被跳过。
     *
     * @throws Exception 测试文件读写失败
     */
    @Test
    void truncatesOnlyTornTailAndRejectsCompleteFrameCorruption() throws Exception {
        XaId xid = new XaId(1, new byte[]{9}, new byte[0]);
        Path file = directory.resolve("mysql.xa");
        try (FileXaRegistry registry = FileXaRegistry.openOrCreate(directory)) {
            registry.preparing(xid, TransactionId.of(21));
        }
        long validSize = Files.size(file);
        Files.write(file, new byte[]{1, 2, 3}, StandardOpenOption.APPEND);
        try (FileXaRegistry repaired = FileXaRegistry.openOrCreate(directory)) {
            assertEquals(validSize, Files.size(file));
            assertEquals(1, repaired.pendingEntries().size());
        }

        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(file, bytes, StandardOpenOption.TRUNCATE_EXISTING);
        assertThrows(DatabaseFatalException.class,
                () -> FileXaRegistry.openOrCreate(directory));
    }

    /** 相反 durable decision、跨 transaction identity 与未 PREPARING 的链均必须拒绝。 */
    @Test
    void rejectsIllegalStateTransitions() {
        XaId xid = new XaId(1, new byte[]{1}, new byte[0]);
        try (FileXaRegistry registry = FileXaRegistry.openOrCreate(directory)) {
            registry.preparing(xid, TransactionId.of(31));
            registry.prepared(xid, TransactionId.of(31));
            registry.decideCommit(xid, TransactionId.of(31));
            assertThrows(DatabaseFatalException.class,
                    () -> registry.decideRollback(xid, TransactionId.of(31)));
            assertThrows(DatabaseFatalException.class,
                    () -> registry.completed(xid, TransactionId.of(32)));
        }
    }

    /**
     * registry append 遇到 channel I/O 失败时必须进入 fail-stop。否则下一次 append 可能接在 partial frame 后，
     * 让重启扫描在 torn tail 前遇到无法解释的完整/半完整组合。
     *
     * @throws Exception 测试通过反射关闭生产 channel 以确定性模拟底层句柄失效
     */
    @Test
    void appendIoFailureFailStopsRegistryUntilReopen() throws Exception {
        XaId xid = new XaId(1, new byte[]{5}, new byte[0]);
        try (FileXaRegistry registry = FileXaRegistry.openOrCreate(directory)) {
            Field channelField = FileXaRegistry.class.getDeclaredField("channel");
            channelField.setAccessible(true);
            ((FileChannel) channelField.get(registry)).close();

            DatabaseFatalException first = assertThrows(DatabaseFatalException.class,
                    () -> registry.preparing(xid, TransactionId.of(41)));
            assertInstanceOf(java.io.IOException.class, first.getCause());
            DatabaseFatalException repeated = assertThrows(DatabaseFatalException.class,
                    registry::pendingEntries);
            assertEquals(first.getCause(), repeated.getCause());
        }
    }
}
