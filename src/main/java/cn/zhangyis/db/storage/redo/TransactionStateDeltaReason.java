package cn.zhangyis.db.storage.redo;

/**
 * 事务状态 logical redo 的原因分类。分类进入 redo 文件用于恢复诊断，但不驱动普通 SQL 事务状态机。
 */
public enum TransactionStateDeltaReason {

    /** 提交路径产生的状态变化。 */
    COMMIT((byte) 1),
    /** 回滚路径产生的状态变化。 */
    ROLLBACK((byte) 2);

    /** redo 文件中的稳定 1 字节原因码；只能追加，不能重排。 */
    private final byte code;

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
