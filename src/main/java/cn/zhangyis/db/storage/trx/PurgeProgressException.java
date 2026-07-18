package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * purge 已越过 LOB/FSP free 或 undo logical-head 写入等无 content-undo 的物理边界后失败。此时普通 Java 异常
 * 回滚只能释放 latch/fix，不能证明页面内容已经恢复；后台 worker 必须 fail-stop，由 crash recovery 依据 redo 和
 * 持久 logical head 判断该记录是否已经完成，禁止在同一进程中把旧 ownership 当作未消费而盲目重试。
 */
public final class PurgeProgressException extends DatabaseFatalException {

    /**
     * 创建并保留记录级 purge 物理进度失败的原始根因。
     *
     * @param message 包含 history first page、undoNo 或 LOB ownership 阶段的可诊断上下文。
     * @param cause   触发失败的 LOB/FSP、undo header、redo admission 或 MTR 提交异常；不能丢失。
     */
    public PurgeProgressException(String message, Throwable cause) {
        super(message, cause);
    }
}
