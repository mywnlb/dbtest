package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.domain.SpaceId;

/** 双槽 control 的已校验不可变快照；next 值均是尚未分配的首个编号。 */
public record DictionaryControlSnapshot(long generation, SpaceId dictionarySpaceId,
                                        long nextSchemaId, long nextTableId, long nextIndexId,
                                        long nextSpaceId, long nextDdlId, long nextDictionaryVersion) {
}
