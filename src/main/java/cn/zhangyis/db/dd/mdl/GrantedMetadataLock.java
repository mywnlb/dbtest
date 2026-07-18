package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** 已授予 MDL 的只读诊断行。
 *
 * @param requestId 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
 * @param owner 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param key 参与 {@code 构造} 的稳定领域标识 {@code MdlKey}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param duration 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
 */
public record GrantedMetadataLock(long requestId, MdlOwnerId owner, MdlKey key,
                                  MdlMode mode, MdlDuration duration) {
}
