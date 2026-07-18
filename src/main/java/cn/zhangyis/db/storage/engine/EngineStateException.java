package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 存储引擎生命周期非法操作异常：在错误的 {@link EngineState} 上调用（如重复 open、CLOSED 上访问）。
 * 属可恢复运行时异常（调用方应修正调用顺序），不破坏实例继续运行的安全性。
 */
public class EngineStateException extends DatabaseRuntimeException {

    /**
     * 创建 {@code EngineStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public EngineStateException(String message) {
        super(message);
    }

    /**
     * 创建 {@code EngineStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public EngineStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
