package cn.zhangyis.db.storage.btree;

/**
 * leaf split 需要向父页插入新的 node pointer，但父页空间不足且当前 B3 切片不实现 parent split。
 * 该异常在重写 leaf 内容之前抛出，避免 MTR rollback 无 content undo 时留下半分裂页。
 */
public class BTreeParentSplitRequiredException extends BTreeSplitRequiredException {

    public BTreeParentSplitRequiredException(String message) {
        super(message);
    }

    public BTreeParentSplitRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
