package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * retirement fence中的一个稳定逻辑资源。
 *
 * @param kind id的解释域；INDEX表示index id，TABLESPACE表示space id
 * @param resourceId 对应kind内已分配的正identity
 */
public record DdlRetiredResource(DdlRetiredResourceKind kind, long resourceId)
        implements Comparable<DdlRetiredResource> {
    public DdlRetiredResource {
        if (kind == null || resourceId <= 0) {
            throw new DatabaseValidationException("invalid DDL retired resource");
        }
    }

    /**
     * 按stable kind code、resource id建立唯一canonical顺序。
     *
     * @param other 同一个fence中的非空资源
     * @return 本对象在canonical顺序中的比较结果
     * @throws DatabaseValidationException other为空时抛出
     */
    @Override
    public int compareTo(DdlRetiredResource other) {
        if (other == null) {
            throw new DatabaseValidationException("DDL retired resource comparison target must not be null");
        }
        int kindOrder = Integer.compare(kind.stableCode(), other.kind.stableCode());
        return kindOrder != 0 ? kindOrder : Long.compare(resourceId, other.resourceId);
    }
}
