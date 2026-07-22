package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DdlId;

import java.util.OptionalLong;

/**
 * tracker生命周期内不可变的Online DDL诊断身份。
 *
 * @param ddlId durable marker/build共用的正identity
 * @param operation 当前operation类型
 * @param tableId 受影响的正table identity
 * @param indexId index operation的正identity；表级operation为0
 * @param tableName 锁内冻结的诊断限定名；恢复无法解析时允许空串
 * @param indexName 索引诊断名；表级或恢复未知时允许空串
 * @param sourceVersion initial aggregate版本；恢复未知时允许0
 * @param targetVersion 计划发布或marker保存的正target版本
 * @param ownerId session/statement/MDL opaque正owner；recovery使用保留值0
 * @param recovery 是否由启动恢复注册而非live coordinator
 * @param statementId 可选的上层statement opaque正identity
 */
public record OnlineDdlOperationIdentity(
        DdlId ddlId, DdlLogOperation operation, long tableId, long indexId,
        String tableName, String indexName, long sourceVersion, long targetVersion,
        long ownerId, boolean recovery, OptionalLong statementId) {

    public OnlineDdlOperationIdentity {
        boolean indexOperation = operation == DdlLogOperation.CREATE_INDEX
                || operation == DdlLogOperation.DROP_INDEX;
        if (ddlId == null || operation == null || tableId <= 0
                || indexId < 0 || indexOperation != (indexId > 0)
                || tableName == null || indexName == null
                || sourceVersion < 0 || targetVersion <= 0 || ownerId < 0
                || statementId == null
                || statementId.isPresent() && statementId.orElseThrow() <= 0) {
            throw new DatabaseValidationException("invalid Online DDL operation identity");
        }
    }
}
