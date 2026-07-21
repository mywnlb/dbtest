package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;

/** 名称确实存在但对象已被 crash recovery 隔离时抛出，区别于普通“不存在”并阻止 SQL 猜测物理文件。 */
public final class TableRecoveryUnavailableException extends DatabaseRuntimeException {

    /** 隔离对象稳定身份。 */
    private final TableId tableId;
    /** committed 恢复状态。 */
    private final TableState tableState;

    /**
     * @param name SQL 请求的规范限定名
     * @param tableId committed DD 表身份
     * @param tableState RECOVERY_UNAVAILABLE 或 RECOVERY_DISCARDED
     */
    public TableRecoveryUnavailableException(QualifiedTableName name, TableId tableId,
                                             TableState tableState) {
        super("table is unavailable after recovery isolation: " + name.canonicalKey()
                + " tableId=" + tableId.value() + " state=" + tableState);
        this.tableId = tableId;
        this.tableState = tableState;
    }

    /** @return 被隔离对象的稳定 DD identity。 */
    public TableId tableId() {
        return tableId;
    }

    /** @return 当前 committed 恢复隔离状态。 */
    public TableState tableState() {
        return tableState;
    }
}
