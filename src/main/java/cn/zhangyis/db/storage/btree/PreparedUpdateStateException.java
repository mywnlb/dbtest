package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * prepared 聚簇 UPDATE 已固定 index page/旧版本证据后未能发布，或 actual row 改变了冻结 key/encoded length。
 * MTR 没有 buffer content undo，调用方必须 fail-stop，不能把该异常降级为普通 statement 重试。
 */
public final class PreparedUpdateStateException extends DatabaseFatalException {

    /**
     * 创建不带底层根因的 prepared UPDATE 致命异常。
     *
     * @param message 描述未发布、重复发布、跨线程或物理形状漂移的领域上下文。
     */
    public PreparedUpdateStateException(String message) { super(message); }

    /**
     * 创建保留底层资源释放/页修改失败的 prepared UPDATE 致命异常。
     *
     * @param message 描述失败发生的 prepared 生命周期阶段。
     * @param cause   原始 B+Tree、MTR 或资源释放异常；不能丢失。
     */
    public PreparedUpdateStateException(String message, Throwable cause) { super(message, cause); }
}
