package cn.zhangyis.db.storage.api.dml;

/**
 * logical unique secondary 的 including-deleted 候选超过教学实现的安全上限。此异常表示当前检查无法证明
 * logical prefix 中不存在尚未读取的 live 冲突；调用方必须回滚当前 statement/transaction，不能使用已扫描前缀发布 entry。
 */
public final class SecondaryUniqueCheckCapacityException extends DmlOperationException {

    /**
     * 创建只包含容量、索引和 logical key 上下文的检查异常。
     *
     * @param message 说明目标索引、logical key 与固定候选上限的非空诊断消息
     */
    public SecondaryUniqueCheckCapacityException(String message) {
        super(message);
    }

    /**
     * 创建保留底层扫描或容量计算根因的检查异常。
     *
     * @param message 说明目标索引、logical key 与固定候选上限的非空诊断消息
     * @param cause   触发容量判断失败的原始异常；包装时必须保留完整 cause 图
     */
    public SecondaryUniqueCheckCapacityException(String message, Throwable cause) {
        super(message, cause);
    }
}
