package cn.zhangyis.db.dd.exception;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** 字典 control/catalog 无法安全解释时的致命异常；启动必须保持 traffic gate 关闭。 */
public class DictionaryCatalogCorruptionException extends DatabaseFatalException {
    public DictionaryCatalogCorruptionException(String message) {
        super(message);
    }

    public DictionaryCatalogCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
