package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;

/**
 * 淘汰端口（buf 侧定义，依赖反转）：当 Buffer Pool 必须淘汰一个脏 victim 帧时，委托本端口把该页经
 * WAL gate + checksum + doublewrite 管线刷干净，从而保证脏页**绝不**在 {@code poolLock} 内、绕过 WAL 直写
 * data file（设计 `innodb-flush-checkpoint-doublewrite-design.md` §6.1 LRU_FLUSH、§7.1、§8.3、§9.2）。
 *
 * <p>由 {@code buf} 定义、{@code flush} 实现（{@code CoordinatedDirtyVictimFlusher} 包 {@code FlushCoordinator}），
 * 保持 {@code buf} 不依赖 {@code flush}（与 {@link PageWriteListener} 同一 DI-seam）。
 *
 * <p><b>返回契约</b>：
 * <ul>
 *   <li>{@code true}：页已确认刷盘清脏，帧可安全复用/淘汰。</li>
 *   <li>{@code false}：本轮未清成——页不脏、对应 redo 尚未 durable、或刷后又被改脏（KEPT_DIRTY）。调用方必须
 *       另选 victim（并把该 PageId 计入本轮 skip set 防空转），**不得**据此直接写盘。</li>
 * </ul>
 *
 * <p><b>失败语义</b>：遇到真正的 IO / doublewrite / force 失败，实现必须抛出领域异常（携带根因），
 * **不能**返回 {@code false} 把盘故障伪装成"可另选"，否则会被上层误当作容量耗尽而掩盖真实故障。
 *
 * <p><b>并发</b>：调用发生在 {@code poolLock} 释放之后；实现内部的 redo 等待、表空间 lease、物理文件锁都不嵌套在
 * {@code poolLock} 之下。当淘汰发生在某 MTR 访问同一表空间期间（调用线程已持该空间共享 lease），实现重入共享
 * lease 安全（{@code TablespaceAccessController} 使用可重入读锁）。
 */
public interface DirtyVictimFlusher {

    /**
     * 尝试把指定脏页经 WAL 安全管线刷干净。
     *
     * @param pageId 待清理的脏 victim 页号。
     * @return true 表示页已清脏可复用；false 表示本轮未清成（不脏/redo 未 durable/又变脏），调用方应另选。
     */
    boolean flushVictim(PageId pageId);
}
