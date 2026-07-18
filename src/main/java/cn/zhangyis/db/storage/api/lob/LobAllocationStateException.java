package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 新 LOB allocation 的 ownership guard 已被跨线程使用、错过 ACTIVE MTR 补偿窗口或补偿失败。此时不能谎报回收
 * 成功，否则可能发布悬空引用或泄漏已提交页链，调用方必须按 outcome-uncertain/fail-stop 处理。
 */
public class LobAllocationStateException extends DatabaseFatalException {

    /**
     * 创建 {@code LobAllocationStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public LobAllocationStateException(String message) {
        super(message);
    }

    /**
     * 创建 {@code LobAllocationStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public LobAllocationStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
