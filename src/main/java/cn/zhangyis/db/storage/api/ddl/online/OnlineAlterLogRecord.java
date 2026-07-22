package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 通用Online ALTER journal的一条不可变frame。 */
public final class OnlineAlterLogRecord {

    private final OnlineAlterLogRecordType type;
    private final long generation;
    private final long sequence;
    private final long transactionId;
    private final byte[] payload;

    public OnlineAlterLogRecord(OnlineAlterLogRecordType type, long generation, long sequence,
                                long transactionId, byte[] payload) {
        if (type == null || generation <= 0 || sequence <= 0 || transactionId < 0 || payload == null
                || (type == OnlineAlterLogRecordType.CANDIDATE) != (transactionId > 0)) {
            throw new DatabaseValidationException("invalid online ALTER log record");
        }
        this.type = type;
        this.generation = generation;
        this.sequence = sequence;
        this.transactionId = transactionId;
        this.payload = payload.clone();
    }

    public OnlineAlterLogRecordType type() { return type; }
    public long generation() { return generation; }
    public long sequence() { return sequence; }
    public long transactionId() { return transactionId; }
    public byte[] payload() { return payload.clone(); }
}
