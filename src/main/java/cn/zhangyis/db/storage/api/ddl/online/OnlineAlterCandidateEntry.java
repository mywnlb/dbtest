package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 通用INPLACE candidate内一个ADD INDEX目标的opaque子payload。
 *
 * @param actionOrdinal manifest有序action中的零基位置
 * @param indexId manifest预留的正目标索引identity
 * @param payload 对应目标`OnlineIndexCandidateCodec`产生的完整非空字节
 */
public record OnlineAlterCandidateEntry(int actionOrdinal, long indexId, byte[] payload) {

    public OnlineAlterCandidateEntry {
        if (actionOrdinal < 0 || indexId <= 0 || payload == null || payload.length == 0) {
            throw new DatabaseValidationException("invalid online ALTER candidate entry");
        }
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
