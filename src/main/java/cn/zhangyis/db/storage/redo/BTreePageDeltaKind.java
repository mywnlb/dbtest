package cn.zhangyis.db.storage.redo;

/**
 * B+Tree 结构页 delta 的稳定磁盘分类。分类只用于恢复期校验和诊断，真正恢复动作仍是把 after-image
 * 覆盖到指定页内 offset；不能把 Java split/merge 调用栈写进 redo 文件。
 */
public enum BTreePageDeltaKind {

    /** FIL prev/next sibling 链字段，当前 v1 已接生产 split/merge/root shrink 写点。 */
    SIBLING_LINKS((byte) 1),
    /** 页格式/header 结构字段预留，后续 root level、page direction 等可迁移到该分类。 */
    PAGE_FORMAT_IMAGE((byte) 2),
    /** internal node pointer 区域预留，用于后续替代节点指针物理字节。 */
    NODE_POINTER_AREA((byte) 3),
    /** root header / level 变化预留，用于后续把 root split/shrink 的结构元数据细化。 */
    ROOT_LEVEL_OR_HEADER((byte) 4);

    /** redo 文件中的稳定 1 字节分类码；只能追加，不能重排。 */
    private final byte code;

    BTreePageDeltaKind(byte code) {
        this.code = code;
    }

    /** 返回 redo 文件中的稳定分类码。 */
    public byte code() {
        return code;
    }

    /**
     * 从 redo payload 的稳定分类码还原枚举。未知分类码表示 redo 文件损坏，恢复必须 fail-closed。
     *
     * @param code redo 文件中的 1 字节分类码。
     * @return 对应分类。
     */
    public static BTreePageDeltaKind fromCode(byte code) {
        for (BTreePageDeltaKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new RedoLogCorruptedException("unknown B+Tree page delta kind code: " + code);
    }
}
