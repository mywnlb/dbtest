package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

/**
 * B+Tree 索引描述符。它是调用路径中的稳定元数据快照，告诉 B+Tree 入口应该从哪个 root page、
 * 用哪个 key 定义和 schema 解释页内记录。
 *
 * @param indexId    索引 id；必须与 root leaf 页 header 的 indexId 一致，否则说明调用方拿错页或元数据过期。
 * @param rootPageId     root page 的物理页号；root split 后仍保持稳定，只提升 rootLevel 并重建 root 内容。
 * @param rootLevel      root 层级；0 表示 root 即 leaf，1 表示 root 保存 node pointer 指向 leaf。
 * @param keyDef         索引 key 定义；leaf 页比较和插入定位的权威 key part 来源。
 * @param schema         leaf 记录 schema；物化结果和比较时的权威列类型来源。
 * @param unique         是否做物理 duplicate key 检查；事务/MVCC 感知的唯一检查后续再接入。
 * @param leafSegment    leaf 页分配所属 segment；leaf-only 调用可为空，split-capable 写路径必须提供。
 * @param nonLeafSegment non-leaf 页分配所属 segment；B3 root 稳定不分配新 non-leaf，仍随元数据携带供后续高度增长。
 */
public record BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                         IndexKeyDef keyDef, TableSchema schema, boolean unique,
                         SegmentRef leafSegment, SegmentRef nonLeafSegment) {

    /**
     * B1/B2 兼容构造器：只描述一个 leaf-only 索引，不携带 segment 信息。
     * 任何需要 split 分配新页的服务必须先校验 segment 字段已经由调用方填充。
     */
    public BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                      IndexKeyDef keyDef, TableSchema schema, boolean unique) {
        this(indexId, rootPageId, rootLevel, keyDef, schema, unique, null, null);
    }

    public BTreeIndex {
        if (indexId < 0) {
            throw new DatabaseValidationException("btree index id must be non-negative: " + indexId);
        }
        if (rootPageId == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("btree index root/keyDef/schema must not be null");
        }
        if (rootLevel < 0) {
            throw new DatabaseValidationException("btree root level must be non-negative: " + rootLevel);
        }
        if (keyDef.indexId() != indexId) {
            throw new DatabaseValidationException("btree index id must match keyDef index id: "
                    + indexId + " vs " + keyDef.indexId());
        }
    }

    /**
     * 返回同一 root/segment/schema 的新层级元数据快照。root split 会重建 root 页内容但不改变 root page id，
     * 因此调用方只需要用该快照继续后续操作。
     */
    public BTreeIndex withRootLevel(int newRootLevel) {
        return new BTreeIndex(indexId, rootPageId, newRootLevel, keyDef, schema, unique,
                leafSegment, nonLeafSegment);
    }

    /**
     * 是否聚簇索引：从 {@link TableSchema#clustered()} 派生。clustered 的单一权威态在 schema，
     * 避免在 BTreeIndex 另立字段造成双重状态不一致；聚簇 leaf conventional 记录据此携带隐藏列
     * （DB_TRX_ID/DB_ROLL_PTR）。node-pointer 派生 schema 恒非 clustered，根/非叶页不带隐藏区。
     */
    public boolean clustered() {
        return schema.clustered();
    }
}
