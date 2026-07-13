package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** LOB 页信封、链路、segment identity 或 CRC 损坏；继续返回值会破坏记录可见性，因此按致命损坏处理。 */
public class LobPageCorruptedException extends DatabaseFatalException {
    public LobPageCorruptedException(String message) {
        super(message);
    }

    public LobPageCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
