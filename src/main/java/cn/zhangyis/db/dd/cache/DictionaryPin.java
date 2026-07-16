package cn.zhangyis.db.dd.cache;

import cn.zhangyis.db.dd.domain.DictionaryVersion;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * 不可变字典版本的 RAII pin。close 幂等；DDL publish 后 stale() 会变为 true，但 value 本身不会被原地修改。
 */
public final class DictionaryPin<T> implements AutoCloseable {

    private final T value;
    private final DictionaryVersion version;
    private final BooleanSupplier stale;
    private final Runnable releaser;
    private final AtomicBoolean closed = new AtomicBoolean();

    DictionaryPin(T value, DictionaryVersion version, BooleanSupplier stale, Runnable releaser) {
        this.value = value;
        this.version = version;
        this.stale = stale;
        this.releaser = releaser;
    }

    public T value() {
        return value;
    }

    public DictionaryVersion version() {
        return version;
    }

    public boolean stale() {
        return stale.getAsBoolean();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            releaser.run();
        }
    }
}
