package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 逻辑 LOB 合法但超过记录 inline 上限，调用方必须先通过 LobStorage 生成 external reference。 */
public class LobExternalizationRequiredException extends DatabaseRuntimeException {
    public LobExternalizationRequiredException(String message) {
        super(message);
    }

    public LobExternalizationRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
