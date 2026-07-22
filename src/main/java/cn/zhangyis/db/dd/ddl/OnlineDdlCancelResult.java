package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * 取消请求的线性化结果与可选诊断快照。
 *
 * @param outcome 稳定返回码
 * @param snapshot registry或durable marker形成的只读投影；NOT_FOUND时为空
 */
public record OnlineDdlCancelResult(
        OnlineDdlCancelOutcome outcome, Optional<OnlineDdlOperationSnapshot> snapshot) {
    public OnlineDdlCancelResult {
        if (outcome == null || snapshot == null
                || (outcome == OnlineDdlCancelOutcome.NOT_FOUND) != snapshot.isEmpty()) {
            throw new DatabaseValidationException("invalid Online DDL cancel result");
        }
    }
}
