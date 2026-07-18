package cn.zhangyis.db.dd.cache;

import cn.zhangyis.db.dd.domain.DictionaryVersion;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * 不可变字典版本的 RAII pin。close 幂等；DDL publish 后 stale() 会变为 true，但 value 本身不会被原地修改。
 */
public final class DictionaryPin<T> implements AutoCloseable {

    /**
     * 当前 pin 持有的已装载字典对象 {@code value}；引用计数释放前身份与版本保持不变，调用方不得将其作为可变权威状态发布。
     */
    private final T value;
    /**
     * 构造或发布时固定的 {@code version} 版本、文件身份或源位置；必须来自当前对象上下文，诊断、并发校验和恢复路径依赖其稳定性。
     */
    private final DictionaryVersion version;
    /**
     * 构造时注入的 {@code stale} 生命周期回调；只允许在类级契约声明的阶段调用，失败必须沿原异常路径传播。
     */
    private final BooleanSupplier stale;
    /**
     * 构造时注入的 {@code releaser} 生命周期回调；只允许在类级契约声明的阶段调用，失败必须沿原异常路径传播。
     */
    private final Runnable releaser;
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * 创建 {@code DictionaryPin}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param value 当前 cache 或 pin 所持有的已装载条目；不得为 {@code null}，引用计数和所有权必须归属当前 cache key
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param stale 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     * @param releaser 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     */
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

    /**
     * 释放本方法拥有的数据字典资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            releaser.run();
        }
    }
}
