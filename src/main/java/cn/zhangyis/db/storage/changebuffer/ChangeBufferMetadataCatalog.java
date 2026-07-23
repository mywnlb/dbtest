package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.record.schema.SecondaryIndexLayout;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Change Buffer exact-version metadata 的进程内目录。前台 DML 会把已经由 DD 固定的表级快照注册进来；
 * 重启后的首次读则可回退到组合根提供的持久 DD resolver。目录只缓存不可变 metadata，不持有页或字典锁。
 */
public final class ChangeBufferMetadataCatalog implements ChangeBufferMetadataResolver {

    /** 已验证的 table/schema/index 三元组；ConcurrentHashMap 只负责可见性，metadata 自身不可变。 */
    private final ConcurrentHashMap<Key, SecondaryIndexMetadata> entries = new ConcurrentHashMap<>();
    /** 可选持久 DD 回退；旧测试组合根没有该能力时允许为空，但未知身份必须 fail-closed。 */
    private final ChangeBufferMetadataResolver fallback;

    /** 创建只依赖运行期注册的目录。 */
    public ChangeBufferMetadataCatalog() {
        this(null);
    }

    /**
     * @param fallback 重启后按稳定三元组读取 committed DD 的可选 resolver；不得返回近似版本
     */
    public ChangeBufferMetadataCatalog(ChangeBufferMetadataResolver fallback) {
        this.fallback = fallback;
    }

    /**
     * 注册一次 exact-version 表级快照；相同三元组只能重复注册等价 metadata，禁止静默覆盖物理 binding。
     *
     * @param metadata 已通过 TableIndexMetadata 构造校验的不可变表级索引聚合
     * @throws ChangeBufferStateException 同一稳定 identity 被映射到不同 descriptor/layout 时抛出
     */
    public void register(TableIndexMetadata metadata) {
        if (metadata == null) {
            throw new DatabaseValidationException("change buffer metadata registration must not be null");
        }
        for (SecondaryIndexMetadata secondary : metadata.secondaryIndexes()) {
            Key key = new Key(metadata.tableId(), metadata.schemaVersion(), secondary.index().indexId());
            entries.compute(key, (ignored, existing) -> {
                if (existing != null && !sameStableBinding(existing, secondary)) {
                    throw new ChangeBufferStateException(
                            "change buffer metadata stable binding changed without DDL barrier: " + key);
                }
                // rootLevel 是每次结构写后的可刷新快照，不是 DD incarnation identity；保留最新调用方观察值。
                return secondary;
            });
        }
    }

    /**
     * 删除已经完成 DDL barrier 的索引缓存。该动作不消费磁盘 Change Buffer 记录；调用者必须先完成 discard/drain。
     *
     * @param tableId 稳定正表 id
     * @param indexId 稳定正二级索引 id
     */
    public void unregisterIndex(long tableId, long indexId) {
        requirePositive(tableId, 1L, indexId);
        entries.keySet().removeIf(key -> key.tableId == tableId && key.indexId == indexId);
    }

    /** {@inheritDoc} */
    @Override
    public SecondaryIndexMetadata resolve(long tableId, long schemaVersion, long indexId) {
        requirePositive(tableId, schemaVersion, indexId);
        Key key = new Key(tableId, schemaVersion, indexId);
        SecondaryIndexMetadata local = entries.get(key);
        if (local != null) {
            return local;
        }
        if (fallback == null) {
            throw new ChangeBufferStateException("change buffer exact-version metadata is unavailable: " + key);
        }
        SecondaryIndexMetadata resolved = fallback.resolve(tableId, schemaVersion, indexId);
        if (resolved == null || resolved.index().indexId() != indexId
                || resolved.index().schema().schemaVersion() != schemaVersion) {
            throw new ChangeBufferStateException("change buffer resolver returned mismatched metadata: " + key);
        }
        SecondaryIndexMetadata raced = entries.putIfAbsent(key, resolved);
        if (raced != null && !sameStableBinding(raced, resolved)) {
            throw new ChangeBufferStateException("change buffer resolver raced with a different binding: " + key);
        }
        return raced == null ? resolved : raced;
    }

    /**
     * 比较跨重启必须稳定的 descriptor/layout 绑定，同时忽略可由 root page header 重新读取的 rootLevel。
     *
     * @param left 已缓存 exact-version 二级 metadata
     * @param right 新 DML 或持久 resolver 返回的同三元组 metadata
     * @return 只有 rootLevel 可以不同时为 {@code true}
     */
    private static boolean sameStableBinding(SecondaryIndexMetadata left, SecondaryIndexMetadata right) {
        if (left == null || right == null || left.logicalUnique() != right.logicalUnique()
                || !sameLayout(left.layout(), right.layout())) {
            return false;
        }
        return left.index().equals(right.index().withRootLevel(left.index().rootLevel()));
    }

    /** 对没有覆写 Object.equals 的不可变 layout 做逐字段值比较，禁止退化成 Java 对象 identity。 */
    private static boolean sameLayout(SecondaryIndexLayout left, SecondaryIndexLayout right) {
        return left.tableSchema().equals(right.tableSchema())
                && left.entrySchema().equals(right.entrySchema())
                && left.physicalKeyDef().equals(right.physicalKeyDef())
                && left.sourceOrdinals().equals(right.sourceOrdinals())
                && left.logicalKeyPartCount() == right.logicalKeyPartCount()
                && left.clusterKeyPartCount() == right.clusterKeyPartCount();
    }

    private static void requirePositive(long tableId, long schemaVersion, long indexId) {
        if (tableId <= 0 || schemaVersion <= 0 || indexId <= 0) {
            throw new DatabaseValidationException("change buffer metadata identities must be positive");
        }
    }

    /** ConcurrentHashMap 使用的稳定三元组；record 自动 equals/hashCode 不依赖可变对象。 */
    private record Key(long tableId, long schemaVersion, long indexId) {
    }
}
