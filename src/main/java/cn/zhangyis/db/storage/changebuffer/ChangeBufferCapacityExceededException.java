package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Change Buffer 已达到配置的 Buffer Pool 页等价量上限。该异常只表示优化路径容量竞争，调用方应在回滚
 * 尚未产生任何全局树修改的 MTR 后回退真实二级索引直写，不能把它升级为用户 DML 失败。
 */
public final class ChangeBufferCapacityExceededException extends DatabaseRuntimeException {

    /**
     * @param message 包含当前 pending 与配置上限的诊断信息
     */
    public ChangeBufferCapacityExceededException(String message) {
        super(message);
    }
}
