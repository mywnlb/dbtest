package cn.zhangyis.db.storage.redo;

/**
 * redo capacity pressure 分级。R2 只计算压力，不直接阻塞 append；后续 page cleaner/redo reservation 可据此触发刷脏或限流。
 */
public enum RedoCapacityPressure {
    /** checkpoint age 低于异步刷脏水位。 */
    NONE,
    /** checkpoint age 达到异步刷脏水位，后台 page cleaner 应提高 flushList 频率。 */
    ASYNC_FLUSH,
    /** checkpoint age 达到同步刷脏水位，前台路径可以等待 checkpoint 进展。 */
    SYNC_FLUSH,
    /** checkpoint age 接近 redo capacity 上限，后续实现应暂停新 redo reservation。 */
    HARD_LIMIT
}
