package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DROP 等待 persistent history 表引用归零超时时抛出的可恢复运行时异常；表必须保持 ACTIVE。 */
public final class TablePurgeBarrierTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建带 table id、当前引用数和 timeout 上下文的可恢复超时异常。
     *
     * @param message 说明目标表、未归零引用数和实际等待边界的诊断信息。
     */
    public TablePurgeBarrierTimeoutException(String message) {
        super(message);
    }

    /**
     * 创建等待超时异常并保留底层计时或 Condition 失败根因。
     *
     * @param message 说明目标表与等待边界的领域诊断信息。
     * @param cause   导致 barrier 无法完成等待的底层异常，不能丢失。
     */
    public TablePurgeBarrierTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
