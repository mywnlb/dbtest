package cn.zhangyis.db.storage.engine;

/**
 * {@link StorageEngine} 生命周期状态（E1）。普通路径为 {@code NEW}→`open()`→{@code OPEN}→`close()`→{@code CLOSED}；
 * 只读校验路径为 {@code NEW}→`open()`→{@code READ_ONLY}→`close()`→{@code CLOSED}。
 * 仅前进，不回退；非法转换或在非 {@code OPEN} 上操作由 {@link StorageEngine} 抛 {@link EngineStateException}。
 */
public enum EngineState {
    /** 已构造、未 open。 */
    NEW,
    /** open 成功、可服务访问器/事务。 */
    OPEN,
    /** READ_ONLY_VALIDATE existing-open 成功，只允许读取恢复报告/gate 状态，不提供普通 storage facade。 */
    READ_ONLY,
    /** 已 close、句柄释放；不可再用。 */
    CLOSED
}
