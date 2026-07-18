package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * MDL 资源 key。resource 必须已经 canonical；CREATE 的目标名即使尚无 TableId 也能先锁住名称身份。
 *
 * @param namespace 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
 * @param resource 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
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

    /**
     * 根据调用参数创建或转换 {@code schema} 返回的 {@code MdlKey}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param canonicalSchema 传给 {@code schema} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code schema} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static MdlKey schema(String canonicalSchema) {
        return new MdlKey(MdlNamespace.SCHEMA, canonicalSchema);
    }

    /**
     * 根据调用参数创建或转换 {@code table} 返回的 {@code MdlKey}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param canonicalQualifiedName 传给 {@code table} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code table} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static MdlKey table(String canonicalQualifiedName) {
        return new MdlKey(MdlNamespace.TABLE, canonicalQualifiedName);
    }

    /**
     * 根据调用参数创建或转换 {@code tablespace} 返回的 {@code MdlKey}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param spaceId 目标表空间的原始数值标识；必须非负、已注册并满足当前生命周期准入条件
     * @return {@code tablespace} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static MdlKey tablespace(int spaceId) {
        if (spaceId < 0) {
            throw new DatabaseValidationException("metadata tablespace key must be non-negative");
        }
        return new MdlKey(MdlNamespace.TABLESPACE, Integer.toUnsignedString(spaceId));
    }

    /**
     * 实现 {@code compareTo} 的稳定值语义；比较只读取输入与本对象，不改变数据字典状态。
     *
     * @param other 参与 {@code compareTo} 的稳定领域标识 {@code MdlKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public int compareTo(MdlKey other) {
        if (other == null) {
            throw new DatabaseValidationException("metadata lock key comparison target must not be null");
        }
        int namespaceOrder = Integer.compare(namespace.rank(), other.namespace.rank());
        return namespaceOrder != 0 ? namespaceOrder : resource.compareTo(other.resource);
    }
}
