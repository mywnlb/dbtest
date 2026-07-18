package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** metadata wait graph 的 owner -> blocker 有向边。
 *
 * @param waiter 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param blocker 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
 */
public record MetadataWaitEdge(MdlOwnerId waiter, MdlOwnerId blocker) {
}
