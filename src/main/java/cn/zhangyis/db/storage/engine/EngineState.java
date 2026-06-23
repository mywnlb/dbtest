package cn.zhangyis.db.storage.engine;

/**
 * {@link StorageEngine} 生命周期状态（E1）。{@code NEW}→`open()`→{@code OPEN}→`close()`→{@code CLOSED}。
 * 仅前进，不回退；非法转换或在非 {@code OPEN} 上操作由 {@link StorageEngine} 抛 {@link EngineStateException}。
 */
public enum EngineState {
    /** 已构造、未 open。 */
    NEW,
    /** open 成功、可服务访问器/事务。 */
    OPEN,
    /** 已 close、句柄释放；不可再用。 */
    CLOSED
}
