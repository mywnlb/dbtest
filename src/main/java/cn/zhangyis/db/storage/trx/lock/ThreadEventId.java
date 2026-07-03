package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Performance Schema 风格的线程事件标识。{@code threadId} 来自 Java 线程，{@code eventId} 由观测层单调分配；
 * 两者共同定位一次等待或授锁诊断事件。{@link #NONE} 仅用于 no-op 观测实现和未接线旧构造器，不能用于真实等待。
 *
 * <p>该值对象是 {@link RowLockEventSink} 观测端口的一部分，定义在 storage 锁层，供 LockManager 与其只读快照 DTO
 * 直接引用；server.lockobs 只做向下依赖的诊断适配，避免底层反向依赖上层观测包。
 *
 * @param threadId Java 线程 id；真实事件必须为正。
 * @param eventId  观测层事件 id；真实事件必须为正。
 */
public record ThreadEventId(long threadId, long eventId) {

    /** 未接观测服务时的空事件。 */
    public static final ThreadEventId NONE = new ThreadEventId(0, 0);

    public ThreadEventId {
        if (threadId < 0) {
            throw new DatabaseValidationException("thread id must be non-negative: " + threadId);
        }
        if (eventId < 0) {
            throw new DatabaseValidationException("event id must be non-negative: " + eventId);
        }
    }

    /** 当前事件是否为真实可诊断事件。 */
    public boolean real() {
        return threadId > 0 && eventId > 0;
    }
}
