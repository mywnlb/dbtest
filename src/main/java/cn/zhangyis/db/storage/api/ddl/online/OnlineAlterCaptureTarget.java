package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 通用INPLACE或SHADOW generation发布给DML admission的不可变capture目标。
 *
 * @param captureId marker/manifest/journal共用identity
 * @param tableId source committed table identity
 * @param changeLog 已force immutable manifest的通用journal
 * @param candidateCodec 多索引或clustered identity纯codec
 */
public record OnlineAlterCaptureTarget(OnlineDdlCaptureId captureId, long tableId,
                                       OnlineAlterChangeLog changeLog,
                                       OnlineDdlCandidateCodec candidateCodec)
        implements OnlineDdlCaptureTarget {

    /** 创建时交叉验证内存目标与durable header，错绑时不得进入gate。 */
    public OnlineAlterCaptureTarget {
        if (captureId == null || tableId <= 0 || changeLog == null || candidateCodec == null
                || !changeLog.header().captureId().equals(captureId)
                || changeLog.header().tableId() != tableId) {
            throw new DatabaseValidationException(
                    "online ALTER capture target does not match durable journal owner");
        }
    }
}
