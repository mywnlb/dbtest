package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 调用方把 LOB 操作交给非 LOB、陈旧或错误 inode segment；在任何 payload 页修改前拒绝。 */
public class LobSegmentMismatchException extends DatabaseRuntimeException {
    /**
     * 创建 {@code LobSegmentMismatchException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public LobSegmentMismatchException(String message) {
        super(message);
    }

    /**
     * 创建 {@code LobSegmentMismatchException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public LobSegmentMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
