package cn.zhangyis.db.storage.btree;

/**
 * 调用方持有的 {@link BTreeIndex} 元数据快照已落后于 root 页实际 level。B+Tree 结构修改后调用方必须使用
 * {@link BTreeInsertResult#indexAfterInsert()} 返回的新快照继续访问，避免用旧路由规则解释新 root。
 */
public class BTreeRootChangedException extends BTreeException {

    /**
     * 创建 {@code BTreeRootChangedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BTreeRootChangedException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BTreeRootChangedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BTreeRootChangedException(String message, Throwable cause) {
        super(message, cause);
    }
}
