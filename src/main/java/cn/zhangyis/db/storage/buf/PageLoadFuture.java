package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.domain.PageId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 单页载入的"完成信号"（设计 §7.1/§7.3）。IO owner 在 pageHashLock+frameMutex 内为某帧建立本对象并注册 LOADING 占位，出锁读盘；
 * 成功发布 CLEAN 后 {@link #complete()}、失败时 {@link #failExceptionally(Throwable)}，唤醒所有等待者。普通失败已先
 * 回收占位并允许重试；claim/写入后的致命失败保留 LOADING 证据并向所有等待者传播同一 fail-stop 异常。
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

    /** IO owner 普通读盘失败或不可回收的发布致命失败后调用，以异常唤醒全部等待者。
     *
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    void failExceptionally(Throwable cause) {
        done.completeExceptionally(cause);
    }

    /**
     * 有界等待载入结束。成功或已回收占位的普通失败正常返回，等待者据此回环重查 residentMap；不可回收的
     * {@link DatabaseFatalException} 原样传播。超时抛 {@link BufferPoolLoadTimeoutException}；被中断时恢复中断位后
     * 抛同类异常。绝不无限期阻塞。
     *
     * @param timeoutNanos 最长等待纳秒数（来自池的 load 超时配置）。
     * @param pageId       仅用于异常上下文。
     * @throws DatabaseFatalException loader 在页面认领/写入或元数据发布后失败时抛出，调用方必须停止普通服务
     * @throws BufferPoolLoadTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     */
    void await(long timeoutNanos, PageId pageId) {
        try {
            done.get(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (ExecutionException loadFailed) {
            Throwable cause = loadFailed.getCause();
            if (cause instanceof DatabaseFatalException fatal) {
                // claim/写入或发布元数据之后的失败不能回收 LOADING frame；等待者必须共同 fail-stop，不能忙循环重试。
                throw fatal;
            }
            // 普通读盘/拦截前失败已回收占位：正常返回并让等待者回环成为新的 IO owner。
        } catch (TimeoutException timedOut) {
            throw new BufferPoolLoadTimeoutException("timed out waiting page load: " + pageId, timedOut);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new BufferPoolLoadTimeoutException("interrupted waiting page load: " + pageId, interrupted);
        }
    }
}
