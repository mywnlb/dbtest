package cn.zhangyis.db.storage.btree;

/**
 * B+Tree 页结构与索引描述不一致。第一片主要用于 root leaf header 校验：页上 index id 或 level
 * 与调用方提供的 {@link BTreeIndex} 不一致时抛出，避免把错误页解释成目标索引。
 */
public class BTreeStructureCorruptedException extends BTreeException {

    public BTreeStructureCorruptedException(String message) {
        super(message);
    }

    public BTreeStructureCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
