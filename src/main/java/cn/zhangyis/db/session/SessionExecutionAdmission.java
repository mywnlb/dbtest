package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

import java.time.Duration;

/**
 * Session 进入一条用户语句前使用的组合根流量门。实现必须允许不同 Session 并行执行，同时让 Engine shutdown 取得
 * 独占 quiescence；permit 生命周期覆盖 parser/binder/executor/transaction cleanup 全链，关闭前不能释放。
 */
public interface SessionExecutionAdmission {

    /**
     * 在有界时间内准入一条语句。返回后调用方拥有一次活动执行计数，必须在同线程 finally 关闭 permit。
     */
    Permit enter(SessionId sessionId, Duration timeout);

    /** 把已破坏同进程继续运行安全性的 fatal 发布给组合根；普通测试/独立 Session 可使用 no-op 实现。 */
    void failClosed(DatabaseFatalException failure);

    /** 不受 Engine 生命周期管理的独立 Session 测试实现。 */
    static SessionExecutionAdmission unrestricted() {
        return UnrestrictedAdmission.INSTANCE;
    }

    /** 活动语句的 RAII 读许可；close 必须无异常且幂等。 */
    interface Permit extends AutoCloseable {
        @Override
        void close();
    }

    /** 仅供不经过 DatabaseEngine 的单元测试和显式独立组装使用。 */
    enum UnrestrictedAdmission implements SessionExecutionAdmission {
        INSTANCE;

        private static final Permit PERMIT = () -> { };

        @Override
        public Permit enter(SessionId sessionId, Duration timeout) {
            return PERMIT;
        }

        @Override
        public void failClosed(DatabaseFatalException failure) {
            // 独立组装没有更上层组合根；Session 自身仍会进入 FAILED。
        }
    }
}
