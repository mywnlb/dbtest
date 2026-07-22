package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 通用manifest中一个需要DML candidate的ADD INDEX目标。
 *
 * @param actionOrdinal manifest action零基位置
 * @param indexId 已预留且写入manifest/descriptor的目标identity
 * @param candidateCodec 冻结目标physical layout的纯codec
 */
public record OnlineAlterIndexTarget(int actionOrdinal, long indexId,
                                     OnlineIndexCandidateCodec candidateCodec) {

    public OnlineAlterIndexTarget {
        if (actionOrdinal < 0 || indexId <= 0 || candidateCodec == null) {
            throw new DatabaseValidationException("invalid online ALTER index target");
        }
    }
}
