package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 批量 secondary range materialization 超过教学实现的安全上限。调用方不得把已读取前缀当作完整 SQL 结果；
 * 可缩小查询范围，或在后续 cursor/fetch 协议落地后重试。
 */
public final class SecondaryRangeCapacityException extends DatabaseRuntimeException {

    public SecondaryRangeCapacityException(String message) {
        super(message);
    }

    public SecondaryRangeCapacityException(String message, Throwable cause) {
        super(message, cause);
    }
}
