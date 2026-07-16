package cn.zhangyis.db.dd.repo;

/**
 * 已在 control 中 durable 的连续身份区间起点。count 来自对应 request；count=0 时起点为 0，调用方不得使用。
 */
public record DictionaryIdAllocation(long firstSchemaId, long firstTableId, long firstIndexId,
                                     int firstSpaceId, long firstDdlId, long dictionaryVersion,
                                     DictionaryIdRequest request, long controlGeneration) {
}
