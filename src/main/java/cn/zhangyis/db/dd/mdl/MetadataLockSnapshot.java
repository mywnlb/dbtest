package cn.zhangyis.db.dd.mdl;

import java.util.List;

/** MDL lock table 与独立 wait graph 的不可变一致性诊断快照。 */
public record MetadataLockSnapshot(List<GrantedMetadataLock> granted,
                                   List<WaitingMetadataLock> waiting,
                                   List<MetadataWaitEdge> waitEdges) {
    public MetadataLockSnapshot {
        granted = List.copyOf(granted);
        waiting = List.copyOf(waiting);
        waitEdges = List.copyOf(waitEdges);
    }
}
