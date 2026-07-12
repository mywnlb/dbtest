package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 字符值无法按 schema 声明的 charset 严格编解码；禁止 replacement character 掩盖索引键变化。 */
public class InvalidCharacterEncodingException extends DatabaseRuntimeException {

    /** 创建字符编码异常。 */
    public InvalidCharacterEncodingException(String message) {
        super(message);
    }

    /** 创建保留底层 {@link java.nio.charset.CharacterCodingException} 根因的字符编码异常。 */
    public InvalidCharacterEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
