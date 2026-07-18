package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 单页载入的"完成信号"（设计 §7.1/§7.3）。IO owner 在 pageHashLock+frameMutex 内为某帧建立本对象并注册 LOADING 占位，出锁读盘；
 * 成功发布 CLEAN 后 {@link #complete()}、失败回收占位后 {@link #failExceptionally(Throwable)}，唤醒所有等待者。
 *
 * <p><b>有界等待</b>（AGENTS 并发约束 / 评审点 4）：命中 LOADING 页的等待者用 {@link #await(long, PageId)} 等候，
 * 必有界：成功或失败均正常返回（失败由调用方回环重试），超时或中断则抛 {@link BufferPoolLoadTimeoutException}，
 * 绝不无限期阻塞。底层用 {@link CompletableFuture}（{@code java.util.concurrent}，非 synchronized/wait/notify）。
 *
 * <p><b>等待者不缓存帧引用</b>：await 只表达"载入已结束"，不返回帧；等待者醒来后必须重取 pageHashLock、按 pageId
 * 重查 residentMap 再决定 fix 或重试——故 await 对成功/失败不作区分，杜绝"拿到已完成 future 却撞淘汰"。
 */
final class PageLoadFuture {

    /** 载入完成信号：complete(null)=成功发布 CLEAN；completeExceptionally=载入失败已回收占位。 */
    private final CompletableFuture<Void> done = new CompletableFuture<>();

    /** IO owner 成功发布 CLEAN 后调用，唤醒全部等待者。 */
    void complete() {
        done.complete(null);
    }

    /** IO owner 读盘失败、已在 Buffer Pool 内部短锁下回收 LOADING 占位后调用，以异常唤醒全部等待者（等待者随后重试）。
     *
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    void failExceptionally(Throwable cause) {
        done.completeExceptionally(cause);
    }

    /**
     * 有界等待载入结束。成功或失败（载入异常）均正常返回——等待者据此回环重查 residentMap；
     * 超时抛 {@link BufferPoolLoadTimeoutException}；被中断时恢复中断位后抛同类异常。绝不无限期阻塞。
     *
     * @param timeoutNanos 最长等待纳秒数（来自池的 load 超时配置）。
     * @param pageId       仅用于异常上下文。
     * @throws BufferPoolLoadTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     */
    void await(long timeoutNanos, PageId pageId) {
        try {
            done.get(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (ExecutionException loadFailed) {
            // 载入失败：吞掉异常正常返回，等待者回环重查——届时占位已被 owner 回收，等待者重试为新 owner。
        } catch (TimeoutException timedOut) {
            throw new BufferPoolLoadTimeoutException("timed out waiting page load: " + pageId, timedOut);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new BufferPoolLoadTimeoutException("interrupted waiting page load: " + pageId, interrupted);
        }
    }
}
