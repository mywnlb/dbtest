package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * CAPTURING admission 冻结的 row-log 目标。对象不包含 DD 或 SQL 类型，只把 build/table/index identity 与
 * 稳定日志端口绑定；同一 build 生命周期内不得更换实例或文件。
 *
 * @param buildId 当前 DDL operation 的正 identity
 * @param tableId 目标表的稳定 DD identity，必须与 manifest owner 一致
 * @param indexId 尚未发布的目标索引 identity，必须与 manifest owner 一致
 * @param changeLog 已经 force immutable manifest 的专属 row-log 端口
 */
public record OnlineIndexCaptureTarget(OnlineIndexBuildId buildId, long tableId, long indexId,
                                       OnlineIndexChangeLog changeLog,
                                       OnlineIndexCandidateCodec candidateCodec)
        implements OnlineDdlCaptureTarget {

    /** 创建后即核对内存 target 与持久 header，避免 candidate 被追加到错误 build。 */
    public OnlineIndexCaptureTarget {
        if (buildId == null || tableId <= 0 || indexId <= 0
                || changeLog == null || candidateCodec == null) {
            throw new DatabaseValidationException("online index capture target identity/log is invalid");
        }
        OnlineIndexLogHeader header = changeLog.header();
        if (!header.buildId().equals(buildId)
                || header.tableId() != tableId || header.indexId() != indexId) {
            throw new DatabaseValidationException(
                    "online index capture target does not match durable row-log owner");
        }
    }

    /** 让既有单索引build进入通用gate而不改变其持久identity。 */
    @Override
    public OnlineDdlCaptureId captureId() {
        return OnlineDdlCaptureId.of(buildId.value());
    }
}
