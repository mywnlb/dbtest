package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 字符值无法按 schema 声明的 charset 严格编解码；禁止 replacement character 掩盖索引键变化。 */
public class InvalidCharacterEncodingException extends DatabaseRuntimeException {

    /** 创建字符编码异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public InvalidCharacterEncodingException(String message) {
        super(message);
    }

    /** 创建保留底层 {@link java.nio.charset.CharacterCodingException} 根因的字符编码异常。
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public InvalidCharacterEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
