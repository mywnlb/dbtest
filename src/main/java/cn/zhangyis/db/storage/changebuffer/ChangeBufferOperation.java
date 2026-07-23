package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Change Buffer 持久 mutation 类型。code 会进入系统表空间记录，既有值不得改号或重排。
 */
public enum ChangeBufferOperation {

    /** 发布一条当前不存在的 live 二级 entry。 */
    INSERT(1),
    /** 把既有 live 二级 entry 置为 delete-marked。 */
    DELETE_MARK(2),
    /** purge 或 rollback 对二级 entry 做物理摘除。 */
    DELETE(3);

    /** 跨重启稳定编码。 */
    private final int code;

    ChangeBufferOperation(int code) {
        this.code = code;
    }

    /**
     * 返回持久 code。
     *
     * @return 1..3 的稳定编码；不等于 ordinal
     */
    public int code() {
        return code;
    }

    /**
     * 从持久 code 还原操作。
     *
     * @param code 从 Change Buffer record 读取的稳定编码
     * @return 对应的已知 mutation 类型
     * @throws DatabaseValidationException code 未知时抛出；恢复调用方必须停止消费该记录
     */
    public static ChangeBufferOperation fromCode(int code) {
        for (ChangeBufferOperation operation : values()) {
            if (operation.code == code) {
                return operation;
            }
        }
        throw new DatabaseValidationException("unknown change buffer operation code: " + code);
    }
}
