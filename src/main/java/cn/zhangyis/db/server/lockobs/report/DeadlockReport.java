package cn.zhangyis.db.server.lockobs.report;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.trx.lock.WaitForEdgeSnapshot;

import java.time.Instant;
import java.util.List;

/**
 * 最近 row-lock deadlock 报告。当前教学实现沿用 LockManager 的“当前等待请求为 victim”策略；
 * 报告只保存不可变等待边和值对象，不保存 Transaction、LockRequest 或 Condition 引用。
 *
 * @param reportId            观测层报告 id。
 * @param detectedAt          发现死锁的时间。
 * @param victimTransactionId victim 事务。
 * @param victimRequestId     victim 等待请求 id。
 * @param edges               检测时的 row-lock wait-for 边。
 * @param summary             简短诊断摘要。
 */
public record DeadlockReport(long reportId, Instant detectedAt, TransactionId victimTransactionId,
                             long victimRequestId, List<WaitForEdgeSnapshot> edges, String summary) {

    public DeadlockReport {
        if (reportId <= 0) {
            throw new DatabaseValidationException("deadlock report id must be positive: " + reportId);
        }
        if (detectedAt == null) {
            throw new DatabaseValidationException("deadlock detectedAt must not be null");
        }
        if (victimTransactionId == null || victimTransactionId.isNone()) {
            throw new DatabaseValidationException("deadlock victim transaction id must be real");
        }
        if (victimRequestId <= 0) {
            throw new DatabaseValidationException("deadlock victim request id must be positive");
        }
        if (edges == null || edges.isEmpty()) {
            throw new DatabaseValidationException("deadlock report edges must not be empty");
        }
        edges = List.copyOf(edges);
        if (summary == null || summary.isBlank()) {
            throw new DatabaseValidationException("deadlock summary must not be blank");
        }
    }
}
