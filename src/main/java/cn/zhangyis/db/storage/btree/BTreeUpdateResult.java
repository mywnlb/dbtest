package cn.zhangyis.db.storage.btree;

/**
 * 聚簇记录整记录替换结果（T1.3e）。{@code replaceClustered} 既用于前向 UPDATE，也用于 rollback 恢复旧 image，
 * 二者都做所有权（DB_TRX_ID/DB_ROLL_PTR）校验后整记录替换，故共用本结果。幂等：未命中、所有权不匹配返回
 * {@code replaced=false} 而非抛异常，使 rollback 在不匹配场景下安全收敛。
 *
 * @param replaced 是否真正替换了一条匹配记录；{@code false} 表示「未命中或非期望版本，未做任何修改」。
 */
public record BTreeUpdateResult(boolean replaced) {
}
