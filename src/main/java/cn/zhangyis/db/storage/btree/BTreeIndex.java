package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.page.PageType;
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
 * @param physicalUnique 是否对完整物理 key 做 duplicate 检查。聚簇 key 和“二级 logical key + 完整主键后缀”
 *                       都必须为 true；DD logical unique 由 {@link SecondaryIndexMetadata} 独立表达。
 * @param leafSegment    leaf 页分配所属 segment；leaf-only 调用可为空，split-capable 写路径必须提供。
 * @param nonLeafSegment non-leaf 页分配所属 segment；B3 root 稳定不分配新 non-leaf，仍随元数据携带供后续高度增长。
 * @param pageType 索引页 envelope 类型；用户/DD 索引为 INDEX，全局 Change Buffer 树为 IBUF_INDEX。
 */
public record BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                         IndexKeyDef keyDef, TableSchema schema, boolean physicalUnique,
                         SegmentRef leafSegment, SegmentRef nonLeafSegment, PageType pageType) {

    /**
     * B1/B2 兼容构造器：只描述一个 leaf-only 索引，不携带 segment 信息。
     * 任何需要 split 分配新页的服务必须先校验 segment 字段已经由调用方填充。
     *
     * @param indexId       写入并核对 root/leaf 页头的稳定索引 id。
     * @param rootPageId    leaf-only 树的稳定 root page identity。
     * @param rootLevel     当前 root level；兼容入口通常为零。
     * @param keyDef        leaf 定位和 duplicate 比较使用的完整 physical key definition。
     * @param schema        leaf record 的 exact-version schema。
     * @param physicalUnique 是否拒绝完整 physical key 重复；不表达 DD logical unique。
     * @throws DatabaseValidationException identity、root level 或必需 metadata 无效时由主构造器抛出。
     */
    public BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                      IndexKeyDef keyDef, TableSchema schema, boolean physicalUnique) {
        this(indexId, rootPageId, rootLevel, keyDef, schema, physicalUnique,
                null, null, PageType.INDEX);
    }

    /**
     * 兼容新增内部索引页类型前的完整 descriptor 构造器；全部既有用户索引保持 {@link PageType#INDEX}。
     *
     * @param indexId 索引稳定 id
     * @param rootPageId 稳定 root 页
     * @param rootLevel 当前 root level
     * @param keyDef 完整物理 key
     * @param schema leaf record schema
     * @param physicalUnique 是否拒绝完整 key 重复
     * @param leafSegment leaf 分配 segment，可为空
     * @param nonLeafSegment non-leaf 分配 segment，可为空
     */
    public BTreeIndex(long indexId, PageId rootPageId, int rootLevel,
                      IndexKeyDef keyDef, TableSchema schema, boolean physicalUnique,
                      SegmentRef leafSegment, SegmentRef nonLeafSegment) {
        this(indexId, rootPageId, rootLevel, keyDef, schema, physicalUnique,
                leafSegment, nonLeafSegment, PageType.INDEX);
    }

    /**
     * 校验索引 descriptor 的稳定 identity 与基础结构不变量。
     *
     * @param indexId        索引稳定 id，必须非负且与 key definition 一致。
     * @param rootPageId     稳定 root page identity，不能为 {@code null}。
     * @param rootLevel      当前结构层级，必须非负。
     * @param keyDef         完整 physical key definition，不能为 {@code null}。
     * @param schema         leaf record schema，不能为 {@code null}。
     * @param physicalUnique 是否拒绝完整 physical identity 重复。
     * @param leafSegment    可选 leaf 分配域；结构写服务会在需要 split 前强制要求它存在。
     * @param nonLeafSegment 可选 non-leaf 分配域；结构写服务会在需要长高前强制要求它存在。
     * @throws DatabaseValidationException id/level 无效、必需字段缺失或 key definition 属于其它索引时抛出。
     */
    public BTreeIndex {
        if (indexId < 0) {
            throw new DatabaseValidationException("btree index id must be non-negative: " + indexId);
        }
        if (rootPageId == null || keyDef == null || schema == null || pageType == null) {
            throw new DatabaseValidationException("btree index root/keyDef/schema/pageType must not be null");
        }
        if (rootLevel < 0) {
            throw new DatabaseValidationException("btree root level must be non-negative: " + rootLevel);
        }
        if (keyDef.indexId() != indexId) {
            throw new DatabaseValidationException("btree index id must match keyDef index id: "
                    + indexId + " vs " + keyDef.indexId());
        }
        if (pageType != PageType.INDEX && pageType != PageType.IBUF_INDEX) {
            throw new DatabaseValidationException("btree page type must be INDEX or IBUF_INDEX: " + pageType);
        }
    }

    /**
     * 返回同一 root/segment/schema 的新层级元数据快照。root split 会重建 root 页内容但不改变 root page id，
     * 因此调用方只需要用该快照继续后续操作。
     *
     * @param newRootLevel 从当前 root page header 读取，或结构写完成后计算出的非负层级。
     * @return 仅 root level 改变，identity、key/schema 与 segment 均保持不变的新 descriptor。
     * @throws DatabaseValidationException 新层级为负数时由主构造器抛出。
     */
    public BTreeIndex withRootLevel(int newRootLevel) {
        return new BTreeIndex(indexId, rootPageId, newRootLevel, keyDef, schema, physicalUnique,
                leafSegment, nonLeafSegment, pageType);
    }

    /**
     * 是否聚簇索引：从 {@link TableSchema#clustered()} 派生。clustered 的单一权威态在 schema，
     * 避免在 BTreeIndex 另立字段造成双重状态不一致；聚簇 leaf conventional 记录据此携带隐藏列
     * （DB_TRX_ID/DB_ROLL_PTR）。node-pointer 派生 schema 恒非 clustered，根/非叶页不带隐藏区。
     *
     * @return leaf schema 声明为 clustered 时返回 {@code true}。
     */
    public boolean clustered() {
        return schema.clustered();
    }
}
