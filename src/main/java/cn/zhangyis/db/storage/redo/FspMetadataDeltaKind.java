package cn.zhangyis.db.storage.redo;

/**
 * FSP metadata delta 的稳定磁盘分类。分类只用于恢复期校验和诊断，真正恢复动作仍是把 after image
 * 覆盖到指定页内 offset；不能把 Java repository 方法名或对象图写进 redo 文件。
 */
public enum FspMetadataDeltaKind {

    /** page0 space header 固定字段，例如 currentSize、freeLimit、nextSegmentId。 */
    SPACE_HEADER_FIELD((byte) 1),
    /** page0 XDES entry 的标量字段，例如 state、owner、prev、next。 */
    XDES_FIELD((byte) 2),
    /** page0 XDES entry 的分配位图字节。 */
    XDES_BITMAP_BYTE((byte) 3),
    /** page2 segment inode 整槽 after image，主要用于 create/drop segment。 */
    INODE_SLOT_IMAGE((byte) 4),
    /** page2 segment inode 标量字段，例如 used/reserved page count。 */
    INODE_FIELD((byte) 5),
    /** page2 segment inode fragment slot。 */
    INODE_FRAGMENT_SLOT((byte) 6),
    /** FLST base 字段，例如 length、first、last。 */
    FLST_BASE_FIELD((byte) 7),
    /** FLST node 字段，例如 prev、next。 */
    FLST_NODE_FIELD((byte) 8);

    /** redo 文件中的稳定 1 字节分类码；只能追加，不能重排。 */
    private final byte code;

    FspMetadataDeltaKind(byte code) {
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
    public static FspMetadataDeltaKind fromCode(byte code) {
        for (FspMetadataDeltaKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new RedoLogCorruptedException("unknown FSP metadata delta kind code: " + code);
    }
}
