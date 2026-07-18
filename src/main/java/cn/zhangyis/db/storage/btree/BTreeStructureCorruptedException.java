package cn.zhangyis.db.storage.btree;

/**
 * B+Tree 页结构与索引描述不一致。第一片主要用于 root leaf header 校验：页上 index id 或 level
 * 与调用方提供的 {@link BTreeIndex} 不一致时抛出，避免把错误页解释成目标索引。
 */
public class BTreeStructureCorruptedException extends BTreeException {

    /**
     * 创建 {@code BTreeStructureCorruptedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BTreeStructureCorruptedException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BTreeStructureCorruptedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BTreeStructureCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
