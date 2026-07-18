package cn.zhangyis.db.storage.btree;

/**
 * unique B+Tree 插入发现已存在相同物理 key。B1/B2 尚未接入事务/MVCC，
 * 因此 delete-marked 的相同 key 也按物理重复处理，避免在无 undo/purge 安全门时插入重复项。
 */
public class BTreeDuplicateKeyException extends BTreeException {

    /**
     * 创建 {@code BTreeDuplicateKeyException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BTreeDuplicateKeyException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BTreeDuplicateKeyException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BTreeDuplicateKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
