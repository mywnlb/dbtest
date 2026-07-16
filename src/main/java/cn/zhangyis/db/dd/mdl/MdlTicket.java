package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

import java.util.concurrent.atomic.AtomicBoolean;

/** 已授予 MDL 的 RAII ticket。close 幂等；upgrade 成功会在同一对象上发布更强 mode。 */
public final class MdlTicket implements AutoCloseable {

    private final long requestId;
    private final MdlOwnerId owner;
    private final MdlKey key;
    private final MdlDuration duration;
    private final MetadataLockManager manager;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile MdlMode mode;

    MdlTicket(long requestId, MdlOwnerId owner, MdlKey key, MdlMode mode, MdlDuration duration,
              MetadataLockManager manager) {
        this.requestId = requestId;
        this.owner = owner;
        this.key = key;
        this.mode = mode;
        this.duration = duration;
        this.manager = manager;
    }

    public MdlOwnerId owner() {
        return owner;
    }

    public MdlKey key() {
        return key;
    }

    public MdlMode mode() {
        return mode;
    }

    public MdlDuration duration() {
        return duration;
    }

    long requestId() {
        return requestId;
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean closeByCaller() {
        return closed.compareAndSet(false, true);
    }

    void closeByManager() {
        closed.set(true);
    }

    void publishMode(MdlMode mode) {
        this.mode = mode;
    }

    @Override
    public void close() {
        manager.release(this);
    }
}
