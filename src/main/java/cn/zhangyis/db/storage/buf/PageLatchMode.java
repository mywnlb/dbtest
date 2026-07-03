package cn.zhangyis.db.storage.buf;

/**
 * 页内容访问模式（page latch 模式）。
 *
 * <p>兼容矩阵（持有者行 × 请求者列，✓=兼容可并发、✗=冲突需等待）：
 * <pre>
 *              SHARED   SHARED_EXCLUSIVE   EXCLUSIVE
 *   SHARED       ✓            ✓               ✗
 *   SHARED_EXCLUSIVE ✓        ✗               ✗
 *   EXCLUSIVE    ✗            ✗               ✗
 * </pre>
 *
 * <p>{@code SHARED_EXCLUSIVE}（SIX，§10.1 乐观下降优化用）表达「读内容 + 独占写意向」：与 {@code SHARED} 并发
 * （多读者可与一个意向者共存），但与其它 {@code SHARED_EXCLUSIVE}/{@code EXCLUSIVE} 互斥（同一时刻只允许一个写意向者）。
 * 它只授予**读**内容权限（写仍须 {@code EXCLUSIVE}）；本片不支持同 MTR 内 {@code SHARED_EXCLUSIVE→EXCLUSIVE} 原地升级
 * （与 {@code SHARED→EXCLUSIVE} 同理会在 {@code ReentrantReadWriteLock} 上自死锁，故被 MTR 拦为领域异常）。
 */
public enum PageLatchMode {
    /** 共享读：多个持有者可并发读同一页内容。 */
    SHARED,
    /** 共享读 + 排他写意向（SIX）：与 SHARED 并发，但排斥其它 SHARED_EXCLUSIVE/EXCLUSIVE；只授予读权限。 */
    SHARED_EXCLUSIVE,
    /** 排他写：独占页内容，写操作必须持有此模式。 */
    EXCLUSIVE
}
