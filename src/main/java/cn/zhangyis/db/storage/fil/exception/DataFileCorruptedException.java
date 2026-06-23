package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 数据文件物理结构损坏异常。例如文件长度非整页对齐，意味着无法安全按页定位。归为致命异常，
 * 普通 IO 不能继续，须交恢复或人工处理。
 */
public class DataFileCorruptedException extends DatabaseFatalException {

    public DataFileCorruptedException(String message) {
        super(message);
    }

    public DataFileCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
