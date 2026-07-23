package cn.zhangyis.db.storage.api.dml;

/**
 * DML 唯一键重复异常。聚簇主键当前仍执行物理 current-read：同 key 的 delete-marked 记录在 purge 前仍算重复；
 * 唯一二级索引则在事务级 logical-key X 锁后重扫当前前缀，仅 live 候选冲突，其它主键的 marked 历史不阻塞复用。
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
