package cn.zhangyis.db.storage.btree;

/**
 * 调用方持有的 {@link BTreeIndex} 元数据快照已落后于 root 页实际 level。B+Tree 结构修改后调用方必须使用
 * {@link BTreeInsertResult#indexAfterInsert()} 返回的新快照继续访问，避免用旧路由规则解释新 root。
 */
public class BTreeRootChangedException extends BTreeException {

    public BTreeRootChangedException(String message) {
        super(message);
    }

    public BTreeRootChangedException(String message, Throwable cause) {
        super(message, cause);
    }
}
