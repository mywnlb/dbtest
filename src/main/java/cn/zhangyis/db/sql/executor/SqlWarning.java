package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 可随成功命令返回的 SQL warning。
 *
 * @param code MySQL 兼容数字代码
 * @param sqlState 五字符 SQLSTATE
 * @param message 含对象身份的稳定诊断文本
 */
public record SqlWarning(int code, String sqlState, String message) {

    public SqlWarning {
        if (code <= 0 || sqlState == null || sqlState.length() != 5
                || message == null || message.isBlank()) {
            throw new DatabaseValidationException("invalid SQL warning");
        }
    }
}
