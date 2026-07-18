package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.TransactionId;

/**
 * 上层 XA 协调器向存储恢复注入的只读决议端口。实现应按持久 XID→TransactionId 映射查询，不能在回调中执行
 * 普通 SQL、获取 page latch 或修改存储页。
 */
@FunctionalInterface
public interface PreparedTransactionDecisionProvider {

    /**
     * 查询一个恢复出的 PREPARED creator 的权威全局决议。
     *
     * @param transactionId 持久 undo header 与 redo 已交叉校验的正存储事务 id
     * @return COMMIT、ROLLBACK 或 UNRESOLVED；不能返回 null
     */
    PreparedTransactionDecision decisionFor(TransactionId transactionId);

    /**
     * 默认 fail-closed provider；未显式注入协调器时任何 PREPARED 都阻止开放流量。
     *
     * @return 对所有事务返回 UNRESOLVED 的无状态 provider
     */
    static PreparedTransactionDecisionProvider unresolved() {
        return transactionId -> PreparedTransactionDecision.UNRESOLVED;
    }
}
