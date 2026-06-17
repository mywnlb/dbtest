package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.storage.buf.FlushPageSnapshot;

/**
 * data file 写入前后的 doublewrite 策略。FlushCoordinator 必须先调用 {@link #beforeDataFileWrite}
 * 并等待其持久化成功，再写 tablespace data file，以满足 doublewrite 防 torn page 的顺序约束。
 */
public interface DoublewriteStrategy {

    /** 当前策略模式。 */
    DoublewriteMode mode();

    /**
     * data file write 前调用；可写入并 fsync doublewrite 副本。
     *
     * @param snapshot 即将写 data file 的页镜像。
     */
    void beforeDataFileWrite(FlushPageSnapshot snapshot);

    /**
     * data file write 成功后调用。F1 无 slot 回收语义，默认 no-op。
     *
     * @param snapshot 已写 data file 的页镜像。
     */
    default void afterDataFileWrite(FlushPageSnapshot snapshot) {
    }
}
