package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表级、可持久化的 SQL 默认选项。Record/storage 不解释 comment；charset/collation 只作为
 * 新列与 CONVERT 的 DD 默认，实际列编码仍以 ColumnTypeDefinition 为准。
 *
 * @param comment 用户表注释；允许空字符串，UTF-8 字节不得超过 2048
 * @param defaultCharsetId 新字符列默认 charset 的正稳定 id
 * @param defaultCollationId 新字符列默认 collation 的正稳定 id
 */
public record TableOptions(String comment, int defaultCharsetId, int defaultCollationId) {

    /** 表注释持久格式上限。 */
    public static final int MAX_COMMENT_BYTES = 2048;

    public TableOptions {
        if (comment == null || comment.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                > MAX_COMMENT_BYTES || defaultCharsetId <= 0 || defaultCollationId <= 0) {
            throw new DatabaseValidationException("table options comment/charset/collation are invalid");
        }
    }

    /** @return 仅供无法携带 schema defaults 的 legacy SDI 兼容读取使用 */
    public static TableOptions legacyDefaults() {
        return new TableOptions("", 1, 1);
    }
}
