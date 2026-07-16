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
 * DD catalog 持久化的 table→tablespace/index/LOB 物理绑定。LOB segment 是表级共享资源：只有 LOB-capable 表的新
 * catalog 才携带；旧 catalog 允许为空，但普通 DML 不得在缺失时临时创建 segment。
 */
public record TableStorageBinding(long tableId, SpaceId spaceId, Path path, List<IndexStorageBinding> indexes,
                                  Optional<SegmentRef> lobSegment) {
    public TableStorageBinding {
        if (tableId <= 0 || spaceId == null || path == null || indexes == null || indexes.isEmpty()
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
