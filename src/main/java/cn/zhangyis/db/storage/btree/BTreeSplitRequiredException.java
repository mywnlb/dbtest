package cn.zhangyis.db.storage.btree;

/**
 * B+Tree 写入遇到当前实现无法在本层完成的 split。leaf-only 服务用它表达 root leaf 已满；
 * split-capable 服务会处理 root/level-1 leaf split，只在更高 parent split 尚未实现时抛子类。
 */
public class BTreeSplitRequiredException extends BTreeException {

    /**
     * 创建 {@code BTreeSplitRequiredException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BTreeSplitRequiredException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BTreeSplitRequiredException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BTreeSplitRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
