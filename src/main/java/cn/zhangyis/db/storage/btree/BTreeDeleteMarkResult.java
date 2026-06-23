package cn.zhangyis.db.storage.btree;

/**
 * 聚簇记录 delete-mark 翻转结果（T1.3f）。{@code setClusteredDeleteMark} 既用于前向逻辑删除（false→true），也用于
 * rollback 取消标记（true→false），二者都做所有权（DB_TRX_ID/DB_ROLL_PTR）校验后外科改 delete 位 + 隐藏列。
 * 幂等：未命中或所有权不匹配返回 {@code changed=false}，不抛异常（非法翻转——已处于目标删除态——则抛领域异常，见
 * {@code setClusteredDeleteMark}）。
 *
 * @param changed 是否真正翻转了一条匹配记录的 delete 位；{@code false} 表示「未命中或非期望版本，未改」。
 */
public record BTreeDeleteMarkResult(boolean changed) {
}
