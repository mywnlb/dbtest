package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DDL log expected phase CAS、重复 prepare 或终态推进失败；调用方停止本次 DDL并交由恢复裁决。 */
public final class DictionaryDdlLogStateException extends DatabaseRuntimeException {

    /**
     * 创建保留 identity、expected/actual phase 等诊断上下文的状态异常。
     *
     * @param message 能定位 ddl id 与非法状态组合的非空诊断消息。
     */
    public DictionaryDdlLogStateException(String message) {
        super(message);
    }
}
