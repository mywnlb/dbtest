package cn.zhangyis.db.dd.repo;

/**
 * 已在 control 中 durable 的连续身份区间起点。count 来自对应 request；count=0 时起点为 0，调用方不得使用。
 *
 * @param firstSchemaId 参与 {@code 构造} 的原始数值身份 {@code firstSchemaId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param firstTableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
 * @param firstIndexId 参与 {@code 构造} 的零基位置 {@code firstIndexId}；必须非负且小于所属页面、集合或持久结构的容量
 * @param firstSpaceId 目标表空间的原始数值标识；必须非负、已注册并满足当前生命周期准入条件
 * @param firstDdlId 参与 {@code 构造} 的原始数值身份 {@code firstDdlId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param dictionaryVersion 参与 {@code 构造} 的单调版本值 {@code dictionaryVersion}；必须非负，回退或与权威快照冲突时拒绝
 * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
 * @param controlGeneration 参与 {@code 构造} 的单调版本值 {@code controlGeneration}；必须非负，回退或与权威快照冲突时拒绝
 */
public record DictionaryIdAllocation(long firstSchemaId, long firstTableId, long firstIndexId,
                                     int firstSpaceId, long firstDdlId, long dictionaryVersion,
                                     DictionaryIdRequest request, long controlGeneration) {
}
