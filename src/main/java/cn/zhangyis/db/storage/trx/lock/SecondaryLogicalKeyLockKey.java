package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * logical secondary key 的稳定事务资源。{@code equalityToken} 已按索引声明的 type/prefix/collation 归一化，
 * 因此比较器认为相等的用户值必然映射到同一资源。unique 检查与 DML 使用 X；non-unique locking read 使用
 * S/X，从而在不依赖瞬时 page/heapNo 的情况下保护完整 logical-prefix range。
 *
 * @param indexId      二级索引 id，用于 LockManager 分片路由。
 * @param equalityToken 类型系统生成的不透明稳定等价 token；不能为空。
 */
public record SecondaryLogicalKeyLockKey(long indexId, String equalityToken) implements TransactionLockKey {

    /**
     * 校验 logical secondary prefix 事务锁 identity。
     *
     * @param indexId      DD 分配的二级索引稳定 id，必须非负。
     * @param equalityToken 已按类型、prefix 与 collation 归一化的非空 token。
     * @throws DatabaseValidationException identity 无效时抛出，防止不同 logical key 错误进入同一空资源。
     */
    public SecondaryLogicalKeyLockKey {
        if (indexId < 0 || equalityToken == null || equalityToken.isEmpty()) {
            throw new DatabaseValidationException("secondary logical lock key fields are invalid");
        }
    }
}
