package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;

import java.util.Optional;

/**
 * 一条 undo identity 对应的权威物理目标。聚簇索引用于 inverse/codec；LOB segment 来自同一精确 table binding，
 * rollback 只能用它校验并释放 INSERT ownership，不能信任 undo reference 自带的 segment identity 作为授权。
 */
public record UndoTargetMetadata(BTreeIndex clusteredIndex, Optional<SegmentRef> lobSegment) {
    public UndoTargetMetadata {
        if (clusteredIndex == null || lobSegment == null || !clusteredIndex.clustered()) {
            throw new DatabaseValidationException("undo target requires clustered index and optional LOB segment");
        }
        if (lobSegment.isPresent()
                && !lobSegment.orElseThrow().spaceId().equals(clusteredIndex.rootPageId().spaceId())) {
            throw new DatabaseValidationException("undo target index/LOB segment must belong to the same space");
        }
    }
}
