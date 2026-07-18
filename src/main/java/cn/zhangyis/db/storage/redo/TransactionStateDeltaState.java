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
    ROLLED_BACK((byte) 5),
    /** XA prepare 已持久化；只追加 code，v1 recovery 无协调器时 fail closed。 */
    PREPARED((byte) 6);

    /** redo 文件中的稳定 1 字节状态码；只能追加，不能重排。 */
    private final byte code;

    /**
     * 创建 {@code TransactionStateDeltaState}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
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
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
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
