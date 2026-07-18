package cn.zhangyis.db.session;

import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.executor.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** 设计 §11.3 的事务模式状态表，不经过 parser 干扰状态机断言。 */
class SessionTransactionPolicyTest {
    @TempDir Path directory;

    /** autocommit、implicit、explicit 的 commit/rollback/begin/set 转换及调用顺序。 */
    @Test
    void followsCompleteTransactionStateTable() {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            RecordingGateway gateway = new RecordingGateway();
            SessionTransactionPolicy policy = new SessionTransactionPolicy(options(true), gateway,
                    dictionary.service, MdlOwnerId.of(1));
            assertEquals(SessionTransactionMode.NONE, policy.mode());

            policy.prepareData(true);
            assertEquals(SessionTransactionMode.AUTOCOMMIT_STATEMENT, policy.mode());
            policy.completeAutocommit(Duration.ofSeconds(1));
            assertEquals(SessionTransactionMode.NONE, policy.mode());

            policy.setAutocommit(false, Duration.ofSeconds(1));
            assertEquals(SessionTransactionMode.IMPLICIT, policy.mode());
            policy.beginExplicit(Duration.ofSeconds(1));
            assertEquals(SessionTransactionMode.EXPLICIT, policy.mode());
            policy.commitAndContinue(Duration.ofSeconds(1));
            assertEquals(SessionTransactionMode.IMPLICIT, policy.mode());
            policy.rollbackAndContinue(Duration.ofSeconds(1));
            assertEquals(SessionTransactionMode.IMPLICIT, policy.mode());
            policy.setAutocommit(true, Duration.ofSeconds(1));
            assertEquals(SessionTransactionMode.NONE, policy.mode());

            int beforeNoop = gateway.events.size();
            policy.commitAndContinue(Duration.ofSeconds(1));
            policy.rollbackAndContinue(Duration.ofSeconds(1));
            assertEquals(beforeNoop, gateway.events.size());
            assertEquals(List.of("begin:RO", "commit", "begin:RW", "commit", "begin:RW", "commit",
                    "begin:RW", "rollback", "begin:RW", "commit"), gateway.events);
        }
    }

    /** close 对活动事务做 full rollback；重复 close 不产生第二次 storage 调用。 */
    @Test
    void closeRollsBackActiveTransactionOnce() {
        try (SessionTestDictionary dictionary = new SessionTestDictionary(directory)) {
            RecordingGateway gateway = new RecordingGateway();
            SessionTransactionPolicy policy = new SessionTransactionPolicy(options(false), gateway,
                    dictionary.service, MdlOwnerId.of(2));
            policy.close();
            policy.close();
            assertEquals(List.of("begin:RW", "rollback"), gateway.events);
        }
    }

    private static SessionOptions options(boolean autocommit) {
        return new SessionOptions(Optional.of("app"), autocommit, SqlIsolationLevel.REPEATABLE_READ,
                SqlDurabilityMode.FLUSH_ON_COMMIT, java.time.ZoneId.of("UTC"), Duration.ofSeconds(2),
                Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1));
    }

    static class RecordingGateway implements SqlStorageGateway {
        final List<String> events = new ArrayList<>();
        int next;
        @Override public SqlTransactionHandle begin(SqlTransactionRequest request) {
            events.add("begin:" + (request.readOnly() ? "RO" : "RW")); return new Handle(++next);
        }
        @Override public SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement,
                                                SqlStatementDeadline deadline) {
            events.add("insert"); return new SqlWriteOutcome(1, false);
        }
        @Override public Optional<SqlRow> selectPoint(SqlTransactionHandle transaction,
                                                     BoundPointSelect statement,
                                                     SqlStatementDeadline deadline) {
            events.add("select"); return Optional.empty();
        }
        @Override public List<SqlRow> selectRange(SqlTransactionHandle transaction,
                                                 BoundSecondaryRangeSelect statement,
                                                 SqlStatementDeadline deadline) {
            events.add("range"); return List.of();
        }
        @Override public SqlWriteOutcome update(SqlTransactionHandle transaction, BoundUpdate statement,
                                                SqlStatementDeadline deadline) {
            events.add("update"); return new SqlWriteOutcome(1, false);
        }
        @Override public SqlWriteOutcome delete(SqlTransactionHandle transaction, BoundDelete statement,
                                                SqlStatementDeadline deadline) {
            events.add("delete"); return new SqlWriteOutcome(1, false);
        }
        @Override public SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request) {
            events.add("commit"); return new SqlCommitOutcome(0, true, 0);
        }
        @Override public SqlRollbackOutcome rollback(SqlTransactionHandle transaction) {
            events.add("rollback"); return new SqlRollbackOutcome(0, 0);
        }
        record Handle(int id) implements SqlTransactionHandle { }
    }
}
