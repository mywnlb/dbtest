package cn.zhangyis.db.storage.btree;

/**
 * current-read 在授予事务锁后多次重新定位仍发现 record/gap 变化。调用方可重启语句或回滚事务；
 * 本异常表示 B+Tree 已释放本轮 stale lock，不会携带 page latch 或 buffer fix。
 */
public class BTreeCurrentReadRelocationException extends BTreeException {

    /**
     * 创建 {@code BTreeCurrentReadRelocationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public BTreeCurrentReadRelocationException(String message) {
        super(message);
    }

    /**
     * 创建 {@code BTreeCurrentReadRelocationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public BTreeCurrentReadRelocationException(String message, Throwable cause) {
        super(message, cause);
    }
}
