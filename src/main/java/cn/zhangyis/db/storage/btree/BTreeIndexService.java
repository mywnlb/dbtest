package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;

import java.util.List;
import java.util.Optional;

/**
 * B+Tree 索引访问门面。实现负责在调用方 MTR 内完成页访问，不自行提交或释放返回资源给上层。
 * B1/B2 的 leaf-only 实现只支持 root leaf；B3 split-capable 实现扩展到 level-1 root-to-leaf 与 sibling scan。
 */
public interface BTreeIndexService {

    /**
     * 在调用方 MTR 内执行点查。实现不得自行 begin/commit MTR，也不得返回持有 page latch 的对象。
     *
     * @param mtr   调用方已开启的 MTR。
     * @param index 索引描述；第一片要求 rootLevel=0。
     * @param key   查找 key。
     * @return 命中时返回已物化记录；缺失或 delete-marked 时返回 empty。
     */
    Optional<BTreeLookupResult> lookup(MiniTransaction mtr, BTreeIndex index, SearchKey key);

    /**
     * 执行 bounded scan。leaf-only 实现仍只扫描 root leaf；split-capable 实现会从 lowerKey 路由到目标 leaf，
     * 并沿 leaf sibling 链继续扫描直到越过上界或达到 limit。
     *
     * @param mtr   调用方已开启的 MTR。
     * @param index 索引描述。
     * @param range 有界扫描范围。
     * @return 按 key 顺序返回的不可变结果列表。
     */
    List<BTreeLookupResult> scanLeaf(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range);

    /**
     * B3 起对外表达真正的 B+Tree range scan；默认委托给历史 scanLeaf 方法以保持 B1/B2 实现源码兼容。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    default List<BTreeLookupResult> scan(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        return scanLeaf(mtr, index, range);
    }

    /**
     * 在调用方 MTR 内插入一条逻辑记录。第一片只支持 root leaf 空间足够的情况；页满时后续 split 片处理。
     *
     * @param mtr    调用方已开启的 MTR。
     * @param index  索引描述；第一片要求 rootLevel=0。
     * @param record 待插入记录，列序必须匹配 index.schema。
     * @return 新记录的短期页内定位。
     */
    BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record);
}
