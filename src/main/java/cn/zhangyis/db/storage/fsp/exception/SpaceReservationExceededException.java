package cn.zhangyis.db.storage.fsp.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 空间预留额度耗尽异常。调用方已经声明当前操作最多会消耗多少 page/extent，但实际分配超出该声明，
 * 继续分配会重新暴露“多页操作半途 ENOSPC”的风险，因此必须在真正分配新页前失败。
 */
public class SpaceReservationExceededException extends DatabaseRuntimeException {

    /**
     * 创建只包含领域诊断消息的预留额度异常。
     *
     * @param message 描述 MTR、表空间和耗尽额度的诊断信息。
     */
    public SpaceReservationExceededException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的预留额度异常。
     *
     * @param message 描述 MTR、表空间和耗尽额度的诊断信息。
     * @param cause 导致预留检查失败的底层异常。
     */
    public SpaceReservationExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
