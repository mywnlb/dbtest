package cn.zhangyis.db.storage.redo;

/**
 * B+Tree 结构页 delta 的稳定磁盘分类。分类只用于恢复期校验和诊断，真正恢复动作仍是把 after-image
 * 覆盖到指定页内 offset；不能把 Java split/merge 调用栈写进 redo 文件。
 */
public enum BTreePageDeltaKind {

    /** FIL prev/next sibling 链字段，当前 v1 已接生产 split/merge/root shrink 写点。 */
    SIBLING_LINKS((byte) 1),
    /** 普通 internal 页的完整 INDEX header after-image。 */
    PAGE_FORMAT_IMAGE((byte) 2),
    /** internal node pointer 的已使用 heap 或 Page Directory after-image。 */
    NODE_POINTER_AREA((byte) 3),
    /** root 完整 INDEX header，或 shrink 到 leaf 后的 level/index identity。 */
    ROOT_LEVEL_OR_HEADER((byte) 4);

    /** redo 文件中的稳定 1 字节分类码；只能追加，不能重排。 */
    private final byte code;

    /**
     * 创建 {@code BTreePageDeltaKind}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
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
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
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
