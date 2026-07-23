package cn.zhangyis.db.storage.changebuffer;

/**
 * 物理 DELETE 的直写前态约束。两种动作在 Change Buffer 中都编码为幂等 DELETE，
 * 但未缓冲时仍须保留 rollback 与 purge 对 live/delete-marked 前态的不同校验。
 */
public enum SecondaryIndexDeleteMode {
    /** rollback 删除本事务刚发布的 live entry；delete-marked 表示恢复证据冲突。 */
    PUBLISHED_LIVE,
    /** purge 删除已经通过版本链证明的 delete-marked entry；live 表示不可安全清理。 */
    DELETE_MARKED
}
