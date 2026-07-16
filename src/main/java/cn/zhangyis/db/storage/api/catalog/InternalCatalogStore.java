package cn.zhangyis.db.storage.api.catalog;

import java.util.List;

/**
 * DD 使用的稳定 storage API。实现可以从 v1 page-framed 文件演进为真正 B+Tree，但上层 repository 不读取
 * FileChannel、BufferFrame 或页格式。
 */
public interface InternalCatalogStore extends AutoCloseable {

    /** 追加并 durable 一个不可分割的逻辑批次；返回单调 batch sequence。 */
    long append(List<CatalogRecord> records);

    /** 读取全部 durable committed 批次的不可变快照。 */
    List<CatalogBatch> readCommittedBatches();

    /** 最近 header generation 证明的 durable 文件边界。 */
    long committedLength();

    @Override
    void close();
}
