package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.EnumSet;
import java.util.Set;

/**
 * 对齐 MySQL 8.0 {@code innodb_change_buffering} 的六种模式。mode 只决定允许尝试的 operation，最终是否缓冲
 * 还必须经过索引属性、目标页 residency、bitmap 和容量检查。
 */
public enum ChangeBufferMode {

    /** 禁止所有新缓冲；既有记录仍须允许 merge/drain。 */
    NONE(0, EnumSet.noneOf(ChangeBufferOperation.class)),
    /** 只缓冲 INSERT。 */
    INSERTS(1, EnumSet.of(ChangeBufferOperation.INSERT)),
    /** 只缓冲 delete-mark。 */
    DELETES(2, EnumSet.of(ChangeBufferOperation.DELETE_MARK)),
    /** 缓冲 INSERT 与 delete-mark。 */
    CHANGES(3, EnumSet.of(ChangeBufferOperation.INSERT, ChangeBufferOperation.DELETE_MARK)),
    /** 只缓冲 purge/rollback 的物理 DELETE。 */
    PURGES(4, EnumSet.of(ChangeBufferOperation.DELETE)),
    /** 缓冲全部受支持 mutation。 */
    ALL(5, EnumSet.allOf(ChangeBufferOperation.class));

    /** 跨重启稳定编码；不得使用 ordinal。 */
    private final int code;
    /** 构造期冻结的操作集合；调用方只做只读 contains。 */
    private final Set<ChangeBufferOperation> operations;

    ChangeBufferMode(int code, Set<ChangeBufferOperation> operations) {
        this.code = code;
        this.operations = Set.copyOf(operations);
    }

    /**
     * 判断本模式是否允许尝试缓冲指定物理变更。
     *
     * @param operation 已由 DML/rollback/purge 决定的二级物理变更；不得为 {@code null}
     * @return 模式包含该操作时为 {@code true}；仍不代表最终 eligibility 成立
     * @throws DatabaseValidationException operation 为空时抛出
     */
    public boolean allows(ChangeBufferOperation operation) {
        if (operation == null) {
            throw new DatabaseValidationException("change buffer operation must not be null");
        }
        return operations.contains(operation);
    }

    /** @return 持久 header 使用的稳定 mode code。 */
    public int code() {
        return code;
    }

    /**
     * 从 header code 还原模式。
     *
     * @param code header 中读取的稳定编码
     * @return 已知模式
     * @throws DatabaseValidationException code 未知时抛出
     */
    public static ChangeBufferMode fromCode(int code) {
        for (ChangeBufferMode mode : values()) {
            if (mode.code == code) {
                return mode;
            }
        }
        throw new DatabaseValidationException("unknown change buffer mode code: " + code);
    }
}
