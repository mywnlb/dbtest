package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.session.SessionId;
import cn.zhangyis.db.session.exception.SessionStateException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** DatabaseEngine statement admission 与 shutdown quiescence 的并发边界测试。 */
class EngineSessionExecutionGateTest {

    /** CLOSING 后拒绝新 reader，且 write quiescence 必须等已准入 reader 释放，不能把 timeout 当成功。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void closingRejectsNewStatementsAndWaitsForAdmittedStatement() throws Exception {
        AtomicReference<DatabaseEngineState> state = new AtomicReference<>(DatabaseEngineState.OPEN);
        EngineSessionExecutionGate gate = new EngineSessionExecutionGate(state::get, failure -> { });
        var active = gate.enter(SessionId.of(1), Duration.ofSeconds(1));
        state.set(DatabaseEngineState.CLOSING);

        assertThrows(SessionStateException.class,
                () -> gate.enter(SessionId.of(2), Duration.ofMillis(50)));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var timedOut = executor.submit(() -> assertThrows(DatabaseRuntimeException.class,
                    () -> gate.awaitQuiescence(Duration.ofMillis(60))));
            timedOut.get(1, TimeUnit.SECONDS);
            active.close();
            try (var ignored = gate.awaitQuiescence(Duration.ofSeconds(1))) {
                assertEquals(DatabaseEngineState.CLOSING, state.get());
            }
        }
    }

    /** Session 报告 fatal 时必须通知组合根 fail-closed，而不是只改变局部 Session 状态。 */
    @Test
    void fatalFailureIsPublishedToEngineOwner() {
        AtomicReference<DatabaseEngineState> state = new AtomicReference<>(DatabaseEngineState.OPEN);
        AtomicReference<DatabaseFatalException> observed = new AtomicReference<>();
        EngineSessionExecutionGate gate = new EngineSessionExecutionGate(state::get, observed::set);
        DatabaseFatalException fatal = new DatabaseFatalException("injected");

        gate.failClosed(fatal);

        assertSame(fatal, observed.get());
    }
}
