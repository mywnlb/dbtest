package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** metadata wait graph 的 owner -> blocker 有向边。 */
public record MetadataWaitEdge(MdlOwnerId waiter, MdlOwnerId blocker) {
}
