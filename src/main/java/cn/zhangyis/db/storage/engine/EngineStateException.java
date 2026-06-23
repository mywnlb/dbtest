package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 存储引擎生命周期非法操作异常：在错误的 {@link EngineState} 上调用（如重复 open、CLOSED 上访问）。
 * 属可恢复运行时异常（调用方应修正调用顺序），不破坏实例继续运行的安全性。
 */
public class EngineStateException extends DatabaseRuntimeException {

    public EngineStateException(String message) {
        super(message);
    }

    public EngineStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
