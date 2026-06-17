package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * mini-transaction memo 栈：LIFO 持有短临界区资源（page latch + buffer fix 等，均表现为 AutoCloseable），
 * commit/rollback/savepoint 时按后进先出释放。仅由属主线程访问，无需自身加锁（设计 §9.2）。
 *
 * <p>每个槽位记为 {@link MemoEntry}：除待释放的 {@link AutoCloseable} 资源外，对 page guard 还额外记录其
 * {@link PageId} 与 {@link PageLatchMode}，使 {@link MiniTransaction#getPage} 能在取 latch 前判断本 MTR 是否
 * 已持有同页的某种 latch，从而拒绝 ReentrantReadWriteLock 不支持的 S→X 升级（否则会自死锁）。非 page 资源
 * 的 pageId/mode 为 null，不参与 {@link #holds} 匹配。
 */
final class MtrMemo {

    /**
     * memo 槽位：待释放资源 + 可选的 page latch 归属信息。
     *
     * @param resource 后进先出释放的资源（page guard 或其它 AutoCloseable）。
     * @param pageId   page guard 所在页；非 page 资源为 null。
     * @param mode     page guard 持有的 latch 模式；非 page 资源为 null。
     */
    private record MemoEntry(AutoCloseable resource, PageId pageId, PageLatchMode mode) {
    }

    /** 资源栈；push/pop 头部，构成 LIFO。 */
    private final Deque<MemoEntry> stack = new ArrayDeque<>();

    /**
     * 压入一个非 page 的待释放资源（如 buffer fix、reservation 等），无 page latch 归属信息。
     *
     * @param resource 非空资源。
     */
    void push(AutoCloseable resource) {
        if (resource == null) {
            throw new DatabaseValidationException("memo resource must not be null");
        }
        stack.push(new MemoEntry(resource, null, null));
    }

    /**
     * 压入一个 page guard，并记录其页与 latch 模式，供 {@link #holds} 检测同页 latch 归属。
     *
     * @param guard  非空 page guard（释放时按 LIFO close）。
     * @param pageId 非空，guard 所在页。
     * @param mode   非空，guard 持有的 latch 模式（S 或 X）。
     */
    void pushPageGuard(PageGuard guard, PageId pageId, PageLatchMode mode) {
        if (guard == null) {
            throw new DatabaseValidationException("memo page guard must not be null");
        }
        if (pageId == null) {
            throw new DatabaseValidationException("memo page id must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("memo page latch mode must not be null");
        }
        stack.push(new MemoEntry(guard, pageId, mode));
    }

    /**
     * 本 MTR 当前是否仍持有指定页的指定模式 latch。只匹配仍在栈中的槽位：被 savepoint 或 commit 释放（pop）的
     * guard 自然不再命中，因此释放后再取相反模式不会被误判为升级。
     *
     * @param pageId 目标页（null 不匹配任何槽位）。
     * @param mode   目标 latch 模式。
     * @return 栈中存在 pageId 与 mode 均相等的 page guard 槽位时返回 true。
     */
    boolean holds(PageId pageId, PageLatchMode mode) {
        for (MemoEntry entry : stack) {
            if (entry.pageId() != null && entry.pageId().equals(pageId) && entry.mode() == mode) {
                return true;
            }
        }
        return false;
    }

    /** 当前栈深，用作 savepoint 标记。 */
    int depth() {
        return stack.size();
    }

    /**
     * 释放到指定深度（LIFO）。当前深度 ≤ targetDepth 时 no-op；逐个 close，即便某资源 close 抛异常也继续释放其余，
     * 最后把首个异常包成 MtrStateException 抛出（其余 addSuppressed），避免泄漏 latch/fix。
     *
     * @param targetDepth 目标深度（非负）。
     */
    void releaseTo(int targetDepth) {
        if (targetDepth < 0) {
            throw new DatabaseValidationException("memo target depth must be non-negative: " + targetDepth);
        }
        Exception firstError = null;
        int failures = 0;
        while (stack.size() > targetDepth) {
            MemoEntry entry = stack.pop();
            try {
                entry.resource().close();
            } catch (Exception e) {
                failures++;
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
            }
        }
        if (firstError != null) {
            throw new MtrStateException("failed to release " + failures + " memo resource(s)", firstError);
        }
    }

    /** 释放全部资源（LIFO）。 */
    void releaseAll() {
        releaseTo(0);
    }

    /**
     * 返回指向 {@code pageId} 的最近一次 X latch 的 page guard；无则视为不变量破坏抛 {@link MtrStateException}。
     * 供 commit 给 touched 页盖 pageLSN（touched 页必有对应 X guard，见 MiniTransaction.rollbackToSavepoint 的保护）。
     */
    PageGuard guardFor(PageId pageId) {
        for (MemoEntry entry : stack) {
            if (entry.pageId() != null && entry.pageId().equals(pageId)
                    && entry.mode() == PageLatchMode.EXCLUSIVE) {
                return (PageGuard) entry.resource();
            }
        }
        throw new MtrStateException("no X page guard in memo for page " + pageId);
    }

    /** 返回深度 &gt; targetDepth（即将被 releaseTo 释放）的各槽位 pageId（非 page 资源跳过）。供 savepoint 回滚前检查 touched。 */
    List<PageId> pageIdsAbove(int targetDepth) {
        List<PageId> result = new ArrayList<>();
        int aboveCount = stack.size() - targetDepth;
        int i = 0;
        for (MemoEntry entry : stack) { // ArrayDeque 迭代 head→tail = 新→旧，头部即栈顶
            if (i++ >= aboveCount) {
                break;
            }
            if (entry.pageId() != null) {
                result.add(entry.pageId());
            }
        }
        return result;
    }
}
