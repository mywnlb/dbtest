package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * MTR 实际持久 redo 超过 begin 前授权上界。此时页面可能已经被修改且 MTR 没有 content undo，进程必须
 * fail-stop，不能 rollback 后把缺失 WAL 的 dirty page 发布给正常流量。
 */
public final class RedoBudgetExceededException extends DatabaseFatalException {

    /** 创建保留用途、预算和实际尺寸的致命诊断。 */
    public RedoBudgetExceededException(String message) {
        super(message);
    }
}
