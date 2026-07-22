package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.concurrent.Callable;

/** 一条 history log 与其 worker 动作；table token 来自 entry 的不可变 affected-table 投影。 */
record PurgeLogWork(HistoryEntry entry, Callable<PurgeLogTaskResult> action) {

    PurgeLogWork {
        if (entry == null || action == null) {
            throw new DatabaseValidationException("purge log work entry/action must not be null");
        }
    }
}
