package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * DD catalog 持久化的 table→tablespace/index/LOB 物理绑定。{@code rowFormatVersion} 是聚簇记录编码格式，
 * metadata-only CREATE INDEX 推进字典版本时必须保持它不变。LOB segment 是表级共享资源：只有 LOB-capable
 * 表的新 catalog 才携带；旧 catalog 允许为空，但普通 DML 不得在缺失时临时创建 segment。
 */
public record TableStorageBinding(long tableId, SpaceId spaceId, Path path, long rowFormatVersion,
                                  List<IndexStorageBinding> indexes, Optional<SegmentRef> lobSegment) {

    /**
     * 源码兼容入口。只用于尚未携带独立格式版本的 bootstrap/测试调用；生产 CREATE、catalog 和 SDI
     * 必须使用六参数构造器显式保存真实版本。
     *
     * @param tableId 表的稳定字典 identity
     * @param spaceId 表空间 identity
     * @param path 表空间规范路径
     * @param indexes 全部逻辑索引的一一对应物理绑定
     * @param lobSegment 可选的表级 LOB segment
     */
    public TableStorageBinding(long tableId, SpaceId spaceId, Path path,
                               List<IndexStorageBinding> indexes, Optional<SegmentRef> lobSegment) {
        this(tableId, spaceId, path, 1L, indexes, lobSegment);
    }

    public TableStorageBinding {
        if (tableId <= 0 || spaceId == null || path == null || rowFormatVersion <= 0
                || indexes == null || indexes.isEmpty()
                || lobSegment == null) {
            throw new DatabaseValidationException("invalid table storage binding");
        }
        path = path.toAbsolutePath().normalize();
        indexes = List.copyOf(indexes);
        Set<Long> indexIds = new HashSet<>();
        Set<cn.zhangyis.db.domain.PageId> rootPages = new HashSet<>();
        for (IndexStorageBinding index : indexes) {
            if (!indexIds.add(index.indexId()) || !rootPages.add(index.rootPageId())) {
                throw new DatabaseValidationException("duplicate index/root identity in table storage binding");
            }
            if (!index.rootPageId().spaceId().equals(spaceId)
                    || !index.leafSegment().spaceId().equals(spaceId)
                    || !index.nonLeafSegment().spaceId().equals(spaceId)) {
                throw new DatabaseValidationException("index root/segment belongs to another tablespace");
            }
            if (index.leafSegment().equals(index.nonLeafSegment())) {
                throw new DatabaseValidationException("index leaf/non-leaf segments must be distinct");
            }
        }
        if (lobSegment.isPresent()) {
            SegmentRef lob = lobSegment.orElseThrow();
            if (!lob.spaceId().equals(spaceId)) {
                throw new DatabaseValidationException("table LOB segment belongs to another tablespace");
            }
            for (IndexStorageBinding index : indexes) {
                if (lob.equals(index.leafSegment()) || lob.equals(index.nonLeafSegment())) {
                    throw new DatabaseValidationException("table LOB segment must be distinct from index segments");
                }
            }
        }
    }
}
