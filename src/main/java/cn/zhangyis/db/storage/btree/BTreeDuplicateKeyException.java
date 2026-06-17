package cn.zhangyis.db.storage.btree;

/**
 * unique B+Tree 插入发现已存在相同物理 key。B1/B2 尚未接入事务/MVCC，
 * 因此 delete-marked 的相同 key 也按物理重复处理，避免在无 undo/purge 安全门时插入重复项。
 */
public class BTreeDuplicateKeyException extends BTreeException {

    public BTreeDuplicateKeyException(String message) {
        super(message);
    }

    public BTreeDuplicateKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
