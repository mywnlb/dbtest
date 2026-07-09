package cn.zhangyis.db.storage.redo;

/**
 * 事务状态 logical redo 使用的稳定磁盘状态枚举。它不直接复用 trx 包的运行时状态机，避免 redo 文件格式
 * 跟 Java 事务实现细节耦合。
 */
public enum TransactionStateDeltaState {

    /** 事务处于运行中。 */
    ACTIVE((byte) 1),
    /** 事务正在提交，提交号已预留但终态尚未发布。 */
    COMMITTING((byte) 2),
    /** 事务已提交。 */
    COMMITTED((byte) 3),
    /** 事务正在回滚，undo 链仍可能被逐条应用。 */
    ROLLING_BACK((byte) 4),
    /** 事务回滚完成。 */
    ROLLED_BACK((byte) 5);

    /** redo 文件中的稳定 1 字节状态码；只能追加，不能重排。 */
    private final byte code;

    TransactionStateDeltaState(byte code) {
        this.code = code;
    }

    /** 返回 redo 文件中的稳定状态码。 */
    public byte code() {
        return code;
    }

    /**
     * 从 redo payload 的稳定状态码还原枚举。未知状态码表示 redo 文件损坏，恢复必须 fail-closed。
     *
     * @param code redo 文件中的 1 字节状态码。
     * @return 对应状态。
     */
    public static TransactionStateDeltaState fromCode(byte code) {
        for (TransactionStateDeltaState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new RedoLogCorruptedException("unknown transaction state delta state code: " + code);
    }
}
