package cn.zhangyis.db.storage.btree;

/**
 * 当前 B+Tree 切片尚不支持的结构形态。B1/B2 只允许 root 即 leaf（level=0），
 * 非叶 root、跨页 scan、split/merge 等后续片在这里显式拒绝，避免静默走错路径。
 */
public class BTreeUnsupportedStructureException extends BTreeException {

    /**
     * 创建 {@code BTreeUnsupportedStructureException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BTreeUnsupportedStructureException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BTreeUnsupportedStructureException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BTreeUnsupportedStructureException(String message, Throwable cause) {
        super(message, cause);
    }
}
