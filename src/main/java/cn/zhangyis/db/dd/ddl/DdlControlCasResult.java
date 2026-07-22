package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * repository control CAS的线性化结果。
 *
 * @param changed 当前调用是否追加并force了新control record
 * @param observedRecord writer fence内最终观察到的record；失败方无需再次无锁读取
 */
public record DdlControlCasResult(boolean changed, DdlLogRecord observedRecord) {
    public DdlControlCasResult {
        if (observedRecord == null) {
            throw new DatabaseValidationException("DDL control CAS observed record must not be null");
        }
    }
}
