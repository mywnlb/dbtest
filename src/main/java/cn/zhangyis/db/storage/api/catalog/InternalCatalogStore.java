package cn.zhangyis.db.storage.api.catalog;

import java.util.List;

/**
 * DD 使用的稳定 storage API。实现可以从 v1 page-framed 文件演进为真正 B+Tree，但上层 repository 不读取
 * FileChannel、BufferFrame 或页格式。
 */
public interface InternalCatalogStore extends AutoCloseable {

    /** 追加并 durable 一个不可分割的逻辑批次；返回单调 batch sequence。
     *
     * @param records 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code append} 从受校验输入或持久字节中得到的 {@code long} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    long append(List<CatalogRecord> records);

    /** 读取全部 durable committed 批次的不可变快照。
     *
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    List<CatalogBatch> readCommittedBatches();

    /** 最近 header generation 证明的 durable 文件边界。
     *
     * @return {@code committedLength} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    long committedLength();

    /**
     * 释放本方法拥有的存储引擎稳定 API资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    void close();
}
