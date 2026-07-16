package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.session.SessionExecutionAdmission;
import cn.zhangyis.db.session.SessionId;
import cn.zhangyis.db.session.exception.SessionBusyException;
import cn.zhangyis.db.session.exception.SessionStateException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * DatabaseEngine 的公平 statement gate。每条用户语句持 read permit，彼此不串行；close 在状态先切 CLOSING 后取得
 * write permit，从而有界等待已准入语句退出，并保证之后没有新语句越过 shutdown 边界。
 *
 * <p>锁顺序固定为本 gate read lock → Session operation lock。Engine close 不持 lifecycle lock 等待 write lock，
 * 且取得 write lock 时已不存在持 Session operation lock 的用户 execute，随后才可安全关闭各 Session。
 */
final class EngineSessionExecutionGate implements SessionExecutionAdmission {

    /** 公平读写锁只表达全实例用户语句与 shutdown 的生命周期关系，不保护页、事务或 MDL 状态。 */
    private final ReentrantReadWriteLock trafficLock = new ReentrantReadWriteLock(true);
    /** Engine 的 volatile 生命周期读取器；read permit 取得后再次核验。 */
    private final Supplier<DatabaseEngineState> stateSupplier;
    /** fatal 发布回调；由 DatabaseEngine 在短 lifecycle 临界区内切 FAILED。 */
    private final Consumer<DatabaseFatalException> fatalHandler;

    EngineSessionExecutionGate(Supplier<DatabaseEngineState> stateSupplier,
                               Consumer<DatabaseFatalException> fatalHandler) {
        if (stateSupplier == null || fatalHandler == null) {
            throw new DatabaseValidationException("engine statement gate collaborators must not be null");
        }
        this.stateSupplier = stateSupplier;
        this.fatalHandler = fatalHandler;
    }

    /** 取得公平 read permit 后复核 OPEN；CLOSING/FAILED/CLOSED 均在执行任何 parser/storage 行为前拒绝。 */
    @Override
    public Permit enter(SessionId sessionId, Duration timeout) {
        if (sessionId == null) {
            throw new DatabaseValidationException("statement admission session id must not be null");
        }
        boolean acquired;
        try {
            acquired = trafficLock.readLock().tryLock(timeoutNanos(timeout), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new SessionBusyException("session " + sessionId.value()
                    + " statement admission interrupted", interrupted);
        }
        if (!acquired) {
            throw new SessionBusyException("session " + sessionId.value()
                    + " statement admission timed out");
        }
        DatabaseEngineState state = stateSupplier.get();
        if (state != DatabaseEngineState.OPEN) {
            trafficLock.readLock().unlock();
            throw new SessionStateException("database engine rejects new statement from state " + state);
        }
        return new ReadPermit(Thread.currentThread());
    }

    /** fatal 不在 gate 锁内执行外部回调，避免状态发布与 shutdown write permit 形成反向锁序。 */
    @Override
    public void failClosed(DatabaseFatalException failure) {
        if (failure == null) {
            throw new DatabaseValidationException("fatal statement failure must not be null");
        }
        fatalHandler.accept(failure);
    }

    /**
     * 取得 shutdown write permit。调用方必须先发布 CLOSING，再调用本方法；timeout 时不返回假成功，storage 必须保持打开。
     */
    QuiescenceLease awaitQuiescence(Duration timeout) {
        boolean acquired;
        try {
            acquired = trafficLock.writeLock().tryLock(timeoutNanos(timeout), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException("engine statement quiescence wait interrupted", interrupted);
        }
        if (!acquired) {
            throw new DatabaseRuntimeException("active statements did not quiesce before engine close timeout");
        }
        return new QuiescenceLease(Thread.currentThread());
    }

    private static long timeoutNanos(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("statement gate timeout must be positive");
        }
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("statement gate timeout is too large", overflow);
        }
    }

    /** read lock 必须由取得它的 execute 线程恰好释放一次。 */
    private final class ReadPermit implements Permit {
        /** ReentrantReadWriteLock 不允许跨线程 unlock。 */
        private final Thread owner;
        /** owner 线程内的幂等终态。 */
        private boolean closed;

        private ReadPermit(Thread owner) {
            this.owner = owner;
        }

        @Override
        public void close() {
            if (closed) return;
            if (Thread.currentThread() != owner) {
                throw new SessionStateException("statement admission permit closed by a non-owner thread");
            }
            closed = true;
            trafficLock.readLock().unlock();
        }
    }

    /** close owner 持有的 write permit；覆盖 Session close 与 storage/DD 资源关闭全链。 */
    final class QuiescenceLease implements AutoCloseable {
        /** write lock 的创建线程。 */
        private final Thread owner;
        /** 同线程幂等 close 标志。 */
        private boolean closed;

        private QuiescenceLease(Thread owner) {
            this.owner = owner;
        }

        @Override
        public void close() {
            if (closed) return;
            if (Thread.currentThread() != owner) {
                throw new DatabaseRuntimeException("engine quiescence lease closed by a non-owner thread");
            }
            closed = true;
            trafficLock.writeLock().unlock();
        }
    }
}
