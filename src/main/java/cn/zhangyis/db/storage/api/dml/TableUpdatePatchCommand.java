package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * 在 FOR_UPDATE 锁定旧版本后应用的列 patch。assignment 必须按 ordinal 严格递增；
 * 未赋值列（包括 external LOB）由 storage 从锁定旧行原样保留。
 *
 * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 * @param metadata 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param clusterKey 参与 {@code 构造} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param assignments 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 * @param lobSegment 可选的 {@code lobSegment}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 * @param lockWaitTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
 */
public record TableUpdatePatchCommand(Transaction transaction, TableIndexMetadata metadata,
                                      SearchKey clusterKey, List<TableColumnAssignment> assignments,
                                      Optional<SegmentRef> lobSegment, Duration lockWaitTimeout) {
    public TableUpdatePatchCommand {
        if (transaction == null || metadata == null || clusterKey == null || assignments == null
                || assignments.isEmpty() || lobSegment == null || lockWaitTimeout == null
                || lockWaitTimeout.isZero() || lockWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("table update patch fields are invalid");
        }
        HashSet<Integer> primaryOrdinals = metadata.clusteredIndex().keyDef().parts().stream()
                .map(part -> part.columnId().value())
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        int previous = -1;
        int width = metadata.clusteredIndex().schema().columnCount();
        if (clusterKey.size() != metadata.clusteredIndex().keyDef().parts().size()) {
            throw new DatabaseValidationException("table update patch requires a complete clustered key");
        }
        for (TableColumnAssignment assignment : assignments) {
            if (assignment.ordinal() >= width || assignment.ordinal() <= previous
                    || primaryOrdinals.contains(assignment.ordinal())) {
                throw new DatabaseValidationException(
                        "table update patch assignments must be ascending, in range and non-primary");
            }
            previous = assignment.ordinal();
        }
        assignments = List.copyOf(assignments);
    }
}
