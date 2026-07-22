package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** Online DDL控制入口未获得admin/system授权；请求不得探测identity或写catalog。 */
public final class OnlineDdlControlAccessException extends DatabaseRuntimeException {
    /** @param message 不包含敏感身份列表的权限诊断 */
    public OnlineDdlControlAccessException(String message) {
        super(message);
    }
}
