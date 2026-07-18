package cn.zhangyis.db.dd.mdl;

import java.util.List;

/** MDL lock table 与独立 wait graph 的不可变一致性诊断快照。
 *
 * @param granted 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param waiting 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param waitEdges 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record MetadataLockSnapshot(List<GrantedMetadataLock> granted,
                                   List<WaitingMetadataLock> waiting,
                                   List<MetadataWaitEdge> waitEdges) {
    public MetadataLockSnapshot {
        granted = List.copyOf(granted);
        waiting = List.copyOf(waiting);
        waitEdges = List.copyOf(waitEdges);
    }
}
