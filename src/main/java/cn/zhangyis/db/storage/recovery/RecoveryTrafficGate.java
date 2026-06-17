package cn.zhangyis.db.storage.recovery;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * crash recovery 用户流量门控。R2 只提供存储层状态对象，不接入 session；后续 session/storage API 入口可查询它决定是否允许请求进入。
 */
public final class RecoveryTrafficGate {

    /** 保护 gate 状态和失败根因。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 当前 gate 状态；只有 OPEN 表示普通流量可进入。 */
    private RecoveryState state = RecoveryState.CLOSED;
    /** fail closed 时保留的根因，用于启动诊断。 */
    private Throwable failure;

    /** 关闭普通流量并标记恢复进行中。 */
    public void closeForRecovery() {
        lock.lock();
        try {
            state = RecoveryState.RECOVERING;
            failure = null;
        } finally {
            lock.unlock();
        }
    }

    /** 恢复成功后开放普通流量。 */
    public void openForUserTraffic() {
        lock.lock();
        try {
            state = RecoveryState.OPEN;
            failure = null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复失败时保持 gate 关闭，并保存根因。
     *
     * @param cause 恢复失败根因。
     */
    public void failClosed(Throwable cause) {
        lock.lock();
        try {
            state = RecoveryState.FAILED;
            failure = cause;
        } finally {
            lock.unlock();
        }
    }

    /** 当前 gate 状态。 */
    public RecoveryState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一次 fail closed 根因。 */
    public Optional<Throwable> lastFailure() {
        lock.lock();
        try {
            return Optional.ofNullable(failure);
        } finally {
            lock.unlock();
        }
    }
}
