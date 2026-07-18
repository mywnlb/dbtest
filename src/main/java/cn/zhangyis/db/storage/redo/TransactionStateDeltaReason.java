package cn.zhangyis.db.storage.redo;

/**
 * 事务状态 logical redo 的原因分类。分类进入 redo 文件并参与 recovery table 的合法终态组合校验，
 * 但不驱动普通 SQL 事务状态机。
 */
public enum TransactionStateDeltaReason {

    /** 提交路径产生的状态变化。 */
    COMMIT((byte) 1),
    /** 回滚路径产生的状态变化。 */
    ROLLBACK((byte) 2),
    /** crash recovery 完成 recovered-active 回滚后产生的终态证据。 */
    RECOVERY_ROLLBACK((byte) 3),
    /** XA phase one 把 ACTIVE 写分支持久化为 PREPARED。 */
    PREPARE((byte) 4),
    /** XA coordinator 决议提交 PREPARED 分支。 */
    PREPARED_COMMIT((byte) 5),
    /** XA coordinator 决议回滚 PREPARED 分支。 */
    PREPARED_ROLLBACK((byte) 6);

    /** redo 文件中的稳定 1 字节原因码；只能追加，不能重排。 */
    private final byte code;

    /**
     * 创建 {@code TransactionStateDeltaReason}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    TransactionStateDeltaReason(byte code) {
        this.code = code;
    }

    /** 返回 redo 文件中的稳定原因码。 */
    public byte code() {
        return code;
    }

    /**
     * 从 redo payload 的稳定原因码还原枚举。未知原因码表示 redo 文件损坏，恢复必须 fail-closed。
     *
     * @param code redo 文件中的 1 字节原因码。
     * @return 对应原因。
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public static TransactionStateDeltaReason fromCode(byte code) {
        for (TransactionStateDeltaReason reason : values()) {
            if (reason.code == code) {
                return reason;
            }
        }
        throw new RedoLogCorruptedException("unknown transaction state delta reason code: " + code);
    }
}
