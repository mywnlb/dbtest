package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** 一次不可变 MDL 申请；ticket 生命周期由 manager 授予后建立。
 *
 * @param owner 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param key 参与 {@code 构造} 的稳定领域标识 {@code MdlKey}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param duration 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
 */
public record MdlRequest(MdlOwnerId owner, MdlKey key, MdlMode mode, MdlDuration duration) {
    public MdlRequest {
        if (owner == null || key == null || mode == null || duration == null) {
            throw new DatabaseValidationException("metadata lock request fields must not be null");
        }
    }
}
