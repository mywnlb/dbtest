package cn.zhangyis.db.storage.btree;

/**
 * 当前 B+Tree 切片尚不支持的结构形态。B1/B2 只允许 root 即 leaf（level=0），
 * 非叶 root、跨页 scan、split/merge 等后续片在这里显式拒绝，避免静默走错路径。
 */
public class BTreeUnsupportedStructureException extends BTreeException {

    public BTreeUnsupportedStructureException(String message) {
        super(message);
    }

    public BTreeUnsupportedStructureException(String message, Throwable cause) {
        super(message, cause);
    }
}
