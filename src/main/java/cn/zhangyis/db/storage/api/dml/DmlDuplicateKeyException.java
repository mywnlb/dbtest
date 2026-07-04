package cn.zhangyis.db.storage.api.dml;

/**
 * 聚簇唯一键重复异常。当前 facade 做的是物理 current-read 唯一检查：包括 delete-marked 当前版本在内，
 * 只要同 key 物理记录仍存在就拒绝再次插入；MVCC 感知的逻辑唯一语义留 SQL/DD 阶段扩展。
 */
public class DmlDuplicateKeyException extends DmlOperationException {

    /**
     * 创建重复键异常。
     *
     * @param message 包含 index/key 上下文的诊断消息。
     */
    public DmlDuplicateKeyException(String message) {
        super(message);
    }

    /**
     * 创建带根因的重复键异常。当前通常由物理 unique check 直接抛出；保留 cause 构造器便于后续
     * MVCC 逻辑唯一检查或 DD 约束检查包装下层异常时不丢根因。
     *
     * @param message 包含 index/key 上下文的诊断消息。
     * @param cause   触发重复键判断的底层异常或校验失败根因。
     */
    public DmlDuplicateKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
