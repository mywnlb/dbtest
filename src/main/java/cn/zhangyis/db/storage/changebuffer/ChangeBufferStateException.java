package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Change Buffer 生命周期、计数或持久树状态不能按请求继续推进时抛出的可恢复领域异常。
 * 调用方应回滚当前 MTR，重新读取 page 3 权威 header；若重读后仍失败，则停止普通写入并进入恢复/诊断流程。
 */
public final class ChangeBufferStateException extends DatabaseRuntimeException {

    /**
     * @param message 包含目标页、序号或生命周期的诊断信息
     */
    public ChangeBufferStateException(String message) {
        super(message);
    }

    /**
     * @param message 包含目标页、序号或生命周期的诊断信息
     * @param cause 导致状态无法推进的原始异常，不能丢失
     */
    public ChangeBufferStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
