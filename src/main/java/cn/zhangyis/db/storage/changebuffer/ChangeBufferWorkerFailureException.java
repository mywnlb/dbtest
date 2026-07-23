package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 后台 Change Buffer merge 不能继续时的实例级致命异常。worker 不猜测错误发生在目标页写入之前还是之后，
 * 因此保留原始 cause、停止自身并要求组合根关闭写入和普通流量；持久 mutation 留给下次 crash recovery 重放。
 */
public final class ChangeBufferWorkerFailureException extends DatabaseFatalException {

    /**
     * @param message 包含后台 merge 生命周期与失败边界的诊断文本
     * @param cause 目标选择、页加载或发布前 merge 抛出的原始领域异常
     */
    public ChangeBufferWorkerFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
