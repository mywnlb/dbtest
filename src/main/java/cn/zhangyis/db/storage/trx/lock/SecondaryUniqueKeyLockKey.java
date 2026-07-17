package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * logical unique secondary key 的事务级排他资源。{@code equalityToken} 已按索引声明的 type/prefix/collation
 * 归一化，因此比较器认为相等的用户值必然映射到同一资源；锁保持到事务终态，串行化“检查 + publish”。
 *
 * @param indexId      二级索引 id，用于 LockManager 分片路由。
 * @param equalityToken 类型系统生成的不透明稳定等价 token；不能为空。
 */
public record SecondaryUniqueKeyLockKey(long indexId, String equalityToken) implements TransactionLockKey {

    /**
     * 校验 logical unique 事务锁 identity。
     *
     * @param indexId      DD 分配的二级索引稳定 id，必须非负。
     * @param equalityToken 已按类型、prefix 与 collation 归一化的非空 token。
     * @throws DatabaseValidationException identity 无效时抛出，防止不同 logical key 错误进入同一空资源。
     */
    public SecondaryUniqueKeyLockKey {
        if (indexId < 0 || equalityToken == null || equalityToken.isEmpty()) {
            throw new DatabaseValidationException("secondary unique lock key fields are invalid");
        }
    }
}
