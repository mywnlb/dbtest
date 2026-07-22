package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * 已完成长度/CRC验证的一条 row-log frame。
 *
 * @param type frame稳定类型
 * @param generation staged tree generation，必须为正
 * @param sequence 文件内严格递增的正序号
 * @param transactionId candidate creator；状态frame使用0
 * @param payload 类型相关opaque payload，不得为null
 */
public record OnlineIndexLogRecord(OnlineIndexLogRecordType type, long generation,
                                   long sequence, long transactionId, byte[] payload) {

    public OnlineIndexLogRecord {
        if (type == null || generation <= 0 || sequence <= 0 || transactionId < 0 || payload == null) {
            throw new DatabaseValidationException("online index log record fields are invalid");
        }
        if (type == OnlineIndexLogRecordType.CANDIDATE && transactionId <= 0
                || type != OnlineIndexLogRecordType.CANDIDATE && transactionId != 0) {
            throw new DatabaseValidationException(
                    "online index log transaction identity does not match record type");
        }
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof OnlineIndexLogRecord that)) return false;
        return generation == that.generation && sequence == that.sequence
                && transactionId == that.transactionId && type == that.type
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(type, generation, sequence, transactionId);
        return 31 * result + Arrays.hashCode(payload);
    }
}
