package cn.zhangyis.db.storage.btree;

/**
 * B+Tree 写入遇到当前实现无法在本层完成的 split。leaf-only 服务用它表达 root leaf 已满；
 * split-capable 服务会处理 root/level-1 leaf split，只在更高 parent split 尚未实现时抛子类。
 */
public class BTreeSplitRequiredException extends BTreeException {

    public BTreeSplitRequiredException(String message) {
        super(message);
    }

    public BTreeSplitRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
