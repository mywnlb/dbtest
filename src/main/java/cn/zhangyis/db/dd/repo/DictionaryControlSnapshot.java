package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.domain.SpaceId;

/** 双槽 control 的已校验不可变快照；next 值均是尚未分配的首个编号。
 *
 * @param generation 参与 {@code 构造} 的单调版本值 {@code generation}；必须非负，回退或与权威快照冲突时拒绝
 * @param dictionarySpaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
 * @param nextSchemaId 参与 {@code 构造} 的原始数值身份 {@code nextSchemaId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param nextTableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
 * @param nextIndexId 参与 {@code 构造} 的零基位置 {@code nextIndexId}；必须非负且小于所属页面、集合或持久结构的容量
 * @param nextSpaceId 目标表空间的原始数值标识；必须非负、已注册并满足当前生命周期准入条件
 * @param nextDdlId 参与 {@code 构造} 的原始数值身份 {@code nextDdlId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param nextDictionaryVersion 参与 {@code 构造} 的单调版本值 {@code nextDictionaryVersion}；必须非负，回退或与权威快照冲突时拒绝
 */
public record DictionaryControlSnapshot(long generation, SpaceId dictionarySpaceId,
                                        long nextSchemaId, long nextTableId, long nextIndexId,
                                        long nextSpaceId, long nextDdlId, long nextDictionaryVersion) {
}
