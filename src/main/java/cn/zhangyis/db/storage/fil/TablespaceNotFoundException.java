package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间不存在异常。用于普通 IO 或 DDL 路径找不到 SpaceId 对应表空间时返回明确领域错误。
 */
public class TablespaceNotFoundException extends DatabaseRuntimeException {

    /**
     * 创建表空间不存在异常。
     *
     * @param message 表空间定位失败的领域描述。
     */
    public TablespaceNotFoundException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的表空间不存在异常。
     *
     * @param message 表空间定位失败的领域描述。
     * @param cause 底层加载失败原因。
     */
    public TablespaceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
