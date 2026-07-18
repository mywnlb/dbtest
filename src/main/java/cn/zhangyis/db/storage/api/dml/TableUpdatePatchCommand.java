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
