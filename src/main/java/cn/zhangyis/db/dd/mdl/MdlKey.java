package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * MDL 资源 key。resource 必须已经 canonical；CREATE 的目标名即使尚无 TableId 也能先锁住名称身份。
 */
public record MdlKey(MdlNamespace namespace, String resource) implements Comparable<MdlKey> {
    public MdlKey {
        if (namespace == null || resource == null || resource.isBlank()) {
            throw new DatabaseValidationException("metadata lock namespace/resource must not be null or blank");
        }
    }

    public static MdlKey global() {
        return new MdlKey(MdlNamespace.GLOBAL, "global");
    }

    public static MdlKey schema(String canonicalSchema) {
        return new MdlKey(MdlNamespace.SCHEMA, canonicalSchema);
    }

    public static MdlKey table(String canonicalQualifiedName) {
        return new MdlKey(MdlNamespace.TABLE, canonicalQualifiedName);
    }

    public static MdlKey tablespace(int spaceId) {
        if (spaceId < 0) {
            throw new DatabaseValidationException("metadata tablespace key must be non-negative");
        }
        return new MdlKey(MdlNamespace.TABLESPACE, Integer.toUnsignedString(spaceId));
    }

    @Override
    public int compareTo(MdlKey other) {
        if (other == null) {
            throw new DatabaseValidationException("metadata lock key comparison target must not be null");
        }
        int namespaceOrder = Integer.compare(namespace.rank(), other.namespace.rank());
        return namespaceOrder != 0 ? namespaceOrder : resource.compareTo(other.resource);
    }
}
